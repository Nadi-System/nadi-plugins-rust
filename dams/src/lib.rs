use nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod dams {
    use anyhow::Result;
    use nadi_core::attributes::AsValue;
    use nadi_core::node::NodeInner;
    use nadi_plugin::nadi_func;

    /// Count the number of dams upstream at each point
    #[nadi_func(outvar = "DAMS_COUNT")]
    fn count_dams(node: &mut NodeInner, outvar: String) -> Result<()> {
        let mut count = 0;
        for i in node.inputs() {
            let n = i.borrow();
            let nc = n
                .attr(&outvar)
                .map(|a| a.as_integer())
                .flatten()
                .unwrap_or(0);
            count += nc;
            if !n.name().starts_with("USGS") {
                count += 1;
            }
        }
        node.set_attr(outvar, toml::Value::Integer(count));
        Ok(())
    }

    /// Count the number of gages upstream at each point
    #[nadi_func(outvar = "GAGES_COUNT")]
    fn count_gages(node: &mut NodeInner, outvar: String) -> Result<()> {
        let mut count = 0;
        for i in node.inputs() {
            let n = i.borrow();
            let nc = n
                .attr(&outvar)
                .map(|a| a.as_integer())
                .flatten()
                .unwrap_or(0);
            count += nc;
            if n.name().starts_with("USGS") {
                count += 1;
            }
        }
        node.set_attr(outvar, toml::Value::Integer(count));
        Ok(())
    }

    /// Propagage the minimum year downstream
    #[nadi_func(write_var = "MIN_YEAR")]
    fn min_year(node: &mut NodeInner, yearattr: String, write_var: String) -> Result<()> {
        let mut min_yr = node.attr(&yearattr).map(|a| a.as_integer()).flatten();
        for i in node.inputs() {
            let n = i.borrow();
            if let Some(yr) = n.attr(&write_var).map(|a| a.as_integer()).flatten() {
                min_yr = match min_yr {
                    Some(m) => {
                        if yr < m {
                            Some(yr)
                        } else {
                            Some(m)
                        }
                    }
                    None => Some(yr),
                }
            }
        }
        if let Some(yr) = min_yr {
            node.set_attr(write_var, toml::Value::Integer(yr));
        }
        Ok(())
    }
}
