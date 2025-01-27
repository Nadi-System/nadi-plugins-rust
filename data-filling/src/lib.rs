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

    #[node_func(method = DataFillMethod::Linear, dtype = "Floats")]
    fn load_csv_fill(
        node: &mut NodeInner,
        /// Name of the timeseries
        name: String,
        /// Template of the CSV file for the nodes
        file: Template,
        /// Names of date column and value column
        columns: (String, String),
        /// date time format, if you only have date, but have time on format string, it will panic
        timefmt: String,
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
