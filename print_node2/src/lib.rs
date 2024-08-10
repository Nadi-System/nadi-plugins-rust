use nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod example {
    use anyhow::Result;
    use nadi_core::{attributes::AsValue, Network, NodeInner};
    use nadi_plugin::nadi_func;

    use crate::errors;

    /** Print the given attr of the node as string.

    This is a basic node funtion, the purpose of this function is
    to demonstrate how node functions can be written. But it might
    be useful in some cases.

    Arguments:
    - attr: String     Attribute to print
    - key: bool        print the attribute name [default: false]
    - sep: String      Separator between attribute name and attribute [default: " = "]
    */
    #[nadi_func(key = false, sep = " = ")]
    fn print_attr(node: &mut NodeInner, attr: String, key: bool, sep: String) -> Result<()> {
        if key {
            print!("{}{}", &attr, sep)
        }
        println!("{}", node.attr(&attr).into_string().unwrap_or_default());
        Ok(())
    }

    /// List all the attributes on the node
    ///
    /// This function lists all the available attributes on the nodes.
    ///
    /// Arguments:
    /// - sep: String   list separator for the attributes [default: ", "]
    #[nadi_func(sep = ", ")]
    fn list_attr(node: &mut NodeInner, sep: String) -> Result<()> {
        println!("{}: {}", node.name(), node.attributes().join(&sep));
        Ok(())
    }

    /// Print node and it's output if present
    ///
    /// Arguments:
    /// - outlet: bool     Whether to print the node without output (outlet) or not [default: false]
    #[nadi_func(outlet = false)]
    fn show_node(node: &mut NodeInner, outlet: bool) -> Result<()> {
        if let Some(out) = node.output() {
            println!("{} -> {}", node.name(), out.borrow().name());
        } else if outlet {
            println!("{}", node.name());
        }
        Ok(())
    }

    /// Print the given attr of the node as name::key=val paired string.
    ///
    /// This function prints the key=val format with the name so that
    /// you can use them as an input to a parser
    ///
    /// Arguments:
    /// attr: String      Attribute to print
    /// prefix: String    Prefix string to add at the beginning of each line [default: ""]
    #[nadi_func(prefix = "")]
    fn print_attr_extra(node: &mut NodeInner, attr: String, prefix: String) -> Result<()> {
        println!(
            "{}{}::{}={}",
            prefix,
            node.name(),
            attr,
            node.attr(&attr).into_string().unwrap_or_default()
        );
        Ok(())
    }

    /** Calculate Error from two attribute values in the network

    It calculates the error using two attribute values from all the nodes.

    Arguments:
    - attr1: String    Attribute value to use as actual value
    - attr2: String    Attribute value to be used to calculate the error
    - error: String    Error type {rmse|nrmse|abserr|nse} [default: rmse]
    */
    #[nadi_func(error = "rmse")]
    fn calc_error(net: &mut Network, attr1: String, attr2: String, error: String) -> Result<()> {
        let obs: Vec<f64> = net
            .nodes()
            .map(|n| {
                n.borrow()
                    .attr(&attr1)
                    .into_loose_float()
                    .unwrap_or(f64::NAN)
            })
            .collect();
        let sim: Vec<f64> = net
            .nodes()
            .map(|n| {
                n.borrow()
                    .attr(&attr2)
                    .into_loose_float()
                    .unwrap_or(f64::NAN)
            })
            .collect();
        let err = match error.as_str() {
            "rmse" => errors::calc_rmse(&obs, &sim),
            "nrmse" => errors::calc_nrmse(&obs, &sim),
            "abserr" => errors::calc_abserr(&obs, &sim),
            "nse" => errors::calc_nse(&obs, &sim),
            _ => return Err(anyhow::Error::msg("Unknown Error type")),
        };
        println!("{}={}", error, err);
        Ok(())
    }
}

mod errors {
    pub fn calc_rmse(obs: &[f64], sim: &[f64]) -> f64 {
        let mut count: usize = 0;
        let mut sum_e: f64 = 0.0;
        obs.iter().zip(sim).for_each(|(kd, cd)| {
            if !kd.is_nan() && !cd.is_nan() {
                sum_e += (cd - kd).powi(2);
                count += 1;
            }
        });
        // not normalized
        (sum_e / count as f64).sqrt()
    }

    pub fn calc_nrmse(obs: &[f64], sim: &[f64]) -> f64 {
        let mut total: f64 = 0.0;
        let mut count: usize = 0;
        let mut sum_e: f64 = 0.0;
        obs.iter().zip(sim).for_each(|(kd, cd)| {
            if !kd.is_nan() && !cd.is_nan() {
                sum_e += (cd - kd).powi(2);
                total += kd;
                count += 1;
            }
        });
        // normalized
        (sum_e / count as f64).sqrt() / (total / count as f64)
    }

    pub fn calc_abserr(obs: &[f64], sim: &[f64]) -> f64 {
        let d = obs.iter().zip(sim).filter_map(|(kd, cd)| {
            if kd.is_nan() || cd.is_nan() {
                None
            } else {
                Some((cd - kd).abs())
            }
        });
        d.clone().sum::<f64>() / (d.count() as f64)
    }

    pub fn calc_nse(obs: &[f64], sim: &[f64]) -> f64 {
        let non_nan = obs.iter().filter(|q| !q.is_nan());
        let mean = non_nan.clone().sum::<f64>() / (non_nan.count() as f64);
        let mut mse: f64 = 0.0;
        let mut denom: f64 = 0.0;
        obs.iter().zip(sim).for_each(|(kd, cd)| {
            if !kd.is_nan() && !cd.is_nan() {
                mse += (cd - kd) * (cd - kd);
                denom += (mean - kd) * (mean - kd)
            }
        });
        1.0 - mse / denom
    }
}
