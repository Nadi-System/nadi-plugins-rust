use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod datafill {
    use super::utils::{DataFillMethod, ExprFunc};
    use nadi_core::abi_stable::external_types::RMutex;
    use nadi_core::abi_stable::std_types::RArc;
    use nadi_core::anyhow::{self, bail, Context};
    use nadi_core::nadi_plugin::node_func;
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use nadi_core::timeseries;
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::HashMap;

    #[node_func(method = DataFillMethod::Linear, dtype = "Floats")]
    fn load_csv_fill(
        node: &mut NodeInner,
        /// Name of the timeseries
        name: String,
        /// Template of the CSV file for the nodes
        file: Template,
        /// date time format, if you only have date, but have time on format string, it will panic
        timefmt: String,
        /// Names of date column and value column
        columns: (String, String),
        // columns: Option<(String, String)>,
        /// Method to use for data filling: forward/backward/linear
        method: DataFillMethod,
        /// DataType to load into timeseries
        dtype: String,
    ) -> anyhow::Result<()> {
        let (dtcol, valcol) = columns;
        let file = node.render(&file)?;
        let df = LazyCsvReader::new(file)
            .with_has_header(true)
            .with_try_parse_dates(true)
            .finish()?;
        // converting the dates to timeline that all timeseries can share
        let dates = df
            .clone()
            .lazy()
            .select([col(&dtcol)
                .dt()
                .timestamp(TimeUnit::Milliseconds)
                .alias("timestamp")])
            .collect()?;

        let dt_col = dates.column("timestamp")?;
        let start = dt_col.min::<i64>()?.context("No minimum date")?;
        let end = dt_col.max::<i64>()?.context("No maximum date")?;
        let step = (end - start) / dt_col.len() as i64;

        let dates = df
            .clone()
            .lazy()
            .select([col(&dtcol).dt().strftime(&timefmt)])
            .collect()?;
        let dt_col = dates.column(&dtcol)?;
        let dates: Vec<&str> = dt_col.str()?.into_no_null_iter().collect();
        let timeline = nadi_core::timeseries::TimeLineInner::new(
            start,
            end,
            step,
            true,
            dates.into_iter().map(String::from).collect(),
            &timefmt,
        );
        let fill: ExprFunc = method.polars_fn()?;
        let values: timeseries::Series = match dtype.as_str() {
            "Floats" => {
                let df2 = df
                    .clone()
                    .lazy()
                    .select([fill(col(&valcol).cast(DataType::Float64))])
                    .collect()?;
                let s = df2.column(&valcol)?;
                let v: Vec<f64> = s.f64()?.into_no_null_iter().collect();
                timeseries::Series::floats(v)
            }
            _ => bail!("{dtype} is not supported or is not a recognized data type"),
        };
        let timeline = RArc::new(RMutex::new(timeline));
        let ts = nadi_core::timeseries::TimeSeries::new(timeline.clone(), values);
        node.set_ts(&name, ts);
        Ok(())
    }

    #[node_func(experiments = 10usize, samples = 100usize)]
    fn datafill_experiment(
        node: &mut NodeInner,
        /// Prefix for name of the series to save metrics on
        name: String,
        /// Template of the CSV file for the nodes
        file: Template,
        // todo: make a String or Int datatype and impl FromAttribute
        /// Names of date column and value column
        columns: Option<(String, String)>,
        /// Number of experiements to run
        experiments: usize,
        /// Number of samples on each experiment
        samples: usize,
    ) -> anyhow::Result<()> {
        let csv = node.render(&file)?;
        let (dtcol, valcol) = match &columns {
            Some((dt, val)) => (col(dt), col(val)),
            None => (nth(0), nth(1)),
        };
        let dtname = columns.as_ref().map(|(d, _)| d.as_str()).unwrap_or("date");
        let mut df = LazyCsvReader::new(csv)
            .with_has_header(columns.is_some())
            .with_try_parse_dates(true)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .with_schema_modify(|mut s| {
                // sometimes it infers it as integer based on first 10 values
                s.set_dtype_at_index(1, DataType::Float64);
                Ok(s)
            })?
            .finish()?
            .with_columns([
                dtcol.clone().alias(dtname),
                valcol.clone().alias(node.name()),
            ])
            .select([col(dtname), col(node.name())])
            .drop_nulls(None);
        node.inputs()
            .iter()
            .try_for_each(|n| -> anyhow::Result<()> {
                let n = n.lock();
                let csv = n.render(&file)?;
                let idf = LazyCsvReader::new(csv)
                    .with_has_header(columns.is_some())
                    .with_try_parse_dates(true)
                    .with_infer_schema_length(Some(10))
                    .with_ignore_errors(true)
                    .with_schema_modify(|mut s| {
                        // sometimes it infers it as integer based on first 10 values
                        s.set_dtype_at_index(1, DataType::Float64);
                        Ok(s)
                    })?
                    .finish()?
                    .with_columns([dtcol.clone().alias(dtname), valcol.clone().alias(n.name())])
                    .select([col(dtname), col(n.name())]);
                df = df.clone().join(
                    idf,
                    [col(dtname)],
                    [col(dtname)],
                    JoinArgs::new(JoinType::Left),
                );
                Ok(())
            })?;
        let df2 = df.collect()?;
        let ht = df2.height();
        let errors: HashMap<&'static str, Vec<f64>> = ["rmse", "nrmse", "abserr", "nse"]
            .into_iter()
            .map(|k| (k, Vec::with_capacity(experiments)))
            .collect();
        let mut fill_methods = [
            (
                "forward",
                DataFillMethod::Strategy(FillNullStrategy::Forward(None)),
                errors.clone(),
            ),
            (
                "backward",
                DataFillMethod::Strategy(FillNullStrategy::Backward(None)),
                errors.clone(),
            ),
            ("linear", DataFillMethod::Linear, errors),
        ];
        for _ in 0..experiments {
            let mut rng = StdRng::from_rng(&mut rand::rng());
            let indices: Vec<i64> = rand::seq::index::sample(&mut rng, ht, samples)
                .iter()
                .map(|i| i as i64)
                .collect();
            let vals = Series::new("nulls".into(), indices);

            let df = df2
                .clone()
                .lazy()
                .with_column(
                    col(dtname)
                        .cum_count(false)
                        .is_in(lit(vals))
                        .alias("sample"),
                )
                .with_column(
                    when(col("sample"))
                        .then(lit(NULL))
                        .otherwise(col(node.name()))
                        .alias("new_vals"),
                )
                .collect()?;

            for (_, method, errors) in &mut fill_methods {
                let fill: ExprFunc = method.polars_fn()?;
                let df = df
                    .clone()
                    .lazy()
                    .with_column(fill(col("new_vals")))
                    .filter(col("sample"))
                    .collect()?;
                let obs: Vec<f64> = df.column(node.name())?.f64()?.into_no_null_iter().collect();
                let sim: Vec<f64> = df.column("new_vals")?.f64()?.into_no_null_iter().collect();
                for (e, errs) in errors {
                    errs.push(calc_error(&obs, &sim, e).expect("should be a known error"));
                }
            }
        }
        for (mname, _, errors) in fill_methods {
            for (e, errs) in errors {
                node.set_series(&format!("{name}_{mname}_{e}"), errs.into());
            }
        }
        Ok(())
    }

    fn calc_error(obs: &[f64], sim: &[f64], error: &str) -> Result<f64, String> {
        let err = match error {
            "rmse" => calc_rmse(obs, sim),
            "nrmse" => calc_nrmse(obs, sim),
            "abserr" => calc_abserr(obs, sim),
            "nse" => calc_nse(obs, sim),
            _ => return Err(String::from("Unknown Error type")),
        };
        Ok(err)
    }

    fn calc_rmse(obs: &[f64], sim: &[f64]) -> f64 {
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

    fn calc_nrmse(obs: &[f64], sim: &[f64]) -> f64 {
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

    fn calc_abserr(obs: &[f64], sim: &[f64]) -> f64 {
        let d = obs.iter().zip(sim).filter_map(|(kd, cd)| {
            if kd.is_nan() || cd.is_nan() {
                None
            } else {
                Some((cd - kd).abs())
            }
        });
        d.clone().sum::<f64>() / (d.count() as f64)
    }

    fn calc_nse(obs: &[f64], sim: &[f64]) -> f64 {
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

mod utils {
    use nadi_core::anyhow;
    use nadi_core::prelude::*;
    use polars::prelude::*;

    pub type ExprFunc = fn(Expr) -> Expr;

    #[derive(Debug)]
    pub enum DataFillMethod {
        Strategy(FillNullStrategy),
        Linear,
        Nearest,
        InputRatio(String),
        OutputRatio(String),
    }

    impl DataFillMethod {
        pub fn polars_fn(&self) -> anyhow::Result<ExprFunc> {
            Ok(match self {
                // All this because we cannot capture s in a fn
                DataFillMethod::Strategy(s) => match s {
                    FillNullStrategy::Forward(None) => |e: Expr| -> Expr { e.forward_fill(None) },
                    FillNullStrategy::Backward(None) => |e: Expr| -> Expr { e.backward_fill(None) },
                    FillNullStrategy::Mean => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Mean) }
                    }
                    FillNullStrategy::Min => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Min) }
                    }
                    FillNullStrategy::Max => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Max) }
                    }
                    FillNullStrategy::Zero => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Zero) }
                    }
                    FillNullStrategy::One => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::One) }
                    }
                    FillNullStrategy::MaxBound => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::MaxBound) }
                    }
                    FillNullStrategy::MinBound => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::MinBound) }
                    }
                    _ => return Err(anyhow::Error::msg("Not implemented")),
                },
                DataFillMethod::Linear => {
                    |e: Expr| -> Expr { e.interpolate(InterpolationMethod::Linear) }
                }
                DataFillMethod::Nearest => {
                    |e: Expr| -> Expr { e.interpolate(InterpolationMethod::Nearest) }
                }
                _ => return Err(anyhow::Error::msg("Not implemented")),
            })
        }
    }

    impl FromAttribute for DataFillMethod {
        fn from_attr(value: &Attribute) -> Option<Self> {
            FromAttribute::try_from_attr(value).ok()
        }
        fn try_from_attr(value: &Attribute) -> Result<Self, String> {
            let strval = String::try_from_attr(value)?;
            let (name, data) = strval.split_once(':').unwrap_or((&strval, ""));
            let no_data = |m: DataFillMethod| -> Result<Self, String> {
                if data.is_empty() {
                    Ok(m)
                } else {
                    Err(format!("Unused part {:?} for Data fill method", data))
                }
            };
            match name {
                "backward" => no_data(Self::Strategy(FillNullStrategy::Backward(None))),
                "forward" => no_data(Self::Strategy(FillNullStrategy::Forward(None))),
                "mean" => no_data(Self::Strategy(FillNullStrategy::Mean)),
                "min" => no_data(Self::Strategy(FillNullStrategy::Min)),
                "max" => no_data(Self::Strategy(FillNullStrategy::Max)),
                "zero" => no_data(Self::Strategy(FillNullStrategy::Zero)),
                "one" => no_data(Self::Strategy(FillNullStrategy::One)),
                "maxbound" => no_data(Self::Strategy(FillNullStrategy::MaxBound)),
                "minbound" => no_data(Self::Strategy(FillNullStrategy::MinBound)),
                "linear" => no_data(Self::Linear),
                "nearest" => no_data(Self::Nearest),
                "input_ratio" | "ratio" => {
                    if data.is_empty() {
                        Err(format!("Data fill method {:?} requires variables", data))
                    } else {
                        Ok(Self::InputRatio(data.to_string()))
                    }
                }
                "output_ratio" | "oratio" => {
                    if data.is_empty() {
                        Err(format!("Data fill method {:?} requires variables", data))
                    } else {
                        Ok(Self::OutputRatio(data.to_string()))
                    }
                }
                x => Err(format!("Data fill method {x:?} not recognized")),
            }
        }
    }
}
