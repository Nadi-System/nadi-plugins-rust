use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod streamflow {
    use nadi_core::nadi_plugin::node_func;
    use nadi_core::node::NodeInner;

    /// Check the given streamflow timeseries for negative values
    #[node_func]
    fn check_negative(node: &mut NodeInner, ts_name: &str) -> Result<i64, String> {
        let ts = node.try_ts(ts_name)?;
        let streamflow: &[f64] = ts.try_values()?;
        let negs = streamflow.iter().filter(|v| **v < 0.0).count();
        if negs > 0 {
            println!("{} {negs} Negative values in streamflow", node.name());
        }
        Ok(negs as i64)
    }
}
