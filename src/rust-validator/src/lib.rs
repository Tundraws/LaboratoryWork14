use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyfunction]
fn validate_window_aggregate(payload: &Bound<'_, PyDict>) -> PyResult<()> {
    let entries: i64 = required_i64(payload, "entries")?;
    let exits: i64 = required_i64(payload, "exits")?;
    let net_flow: i64 = required_i64(payload, "net_flow")?;
    let events_count: i64 = required_i64(payload, "events_count")?;
    let average_per_tick: f64 = required_f64(payload, "average_per_tick")?;

    if entries < 0 || exits < 0 || events_count < 0 {
        return Err(PyValueError::new_err("counters must be non-negative"));
    }
    if average_per_tick < 0.0 {
        return Err(PyValueError::new_err("average_per_tick must be non-negative"));
    }
    if entries - exits != net_flow {
        return Err(PyValueError::new_err("net_flow must equal entries minus exits"));
    }
    Ok(())
}

#[pymodule]
fn metro_validator(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(validate_window_aggregate, module)?)?;
    Ok(())
}

fn required_i64(payload: &Bound<'_, PyDict>, key: &str) -> PyResult<i64> {
    payload
        .get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("{key} is required")))?
        .extract::<i64>()
}

fn required_f64(payload: &Bound<'_, PyDict>, key: &str) -> PyResult<f64> {
    payload
        .get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("{key} is required")))?
        .extract::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn accepts_consistent_flow() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let payload = PyDict::new_bound(py);
            payload.set_item("entries", 12).unwrap();
            payload.set_item("exits", 5).unwrap();
            payload.set_item("net_flow", 7).unwrap();
            payload.set_item("events_count", 3).unwrap();
            payload.set_item("average_per_tick", 5.6).unwrap();

            assert!(validate_window_aggregate(&payload).is_ok());
        });
    }

    #[test]
    fn rejects_inconsistent_flow() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let payload = PyDict::new_bound(py);
            payload.set_item("entries", 12).unwrap();
            payload.set_item("exits", 5).unwrap();
            payload.set_item("net_flow", 0).unwrap();
            payload.set_item("events_count", 3).unwrap();
            payload.set_item("average_per_tick", 5.6).unwrap();

            assert!(validate_window_aggregate(&payload).is_err());
        });
    }
}
