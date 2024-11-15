use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod gnuplot {
    use nadi_core::abi_stable::std_types::Tuple2;
    use nadi_core::attrs::FromAttribute;
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use std::{
        fs::File,
        io::Write,
        path::{Path, PathBuf},
    };

    #[derive(Debug, Default)]
    struct GnuplotConfig {
        outfile: Option<PathBuf>,
        terminal: Option<String>,
        csv: bool,
        preamble: String,
        // TODO add more config options for gnuplot
    }

    // If you need custom values, you can implement FromAttribute, or
    // FromAttributeRelaxed for your type; this will allow any custom
    // type to be used in your function.
    impl FromAttribute for GnuplotConfig {
        fn from_attr(value: &Attribute) -> Option<Self> {
            Self::try_from_attr(value).ok()
        }

        fn try_from_attr(value: &Attribute) -> Result<Self, String> {
            let tab = AttrMap::try_from_attr(value)?;
            let mut config = Self::default();
            for Tuple2(k, v) in &tab {
                match k.as_str() {
                    "outfile" => {
                        config.outfile = Some(PathBuf::try_from_attr(v)?);
                    }
                    "terminal" => {
                        config.terminal = Some(String::try_from_attr(v)?);
                    }
                    "csv" => {
                        config.csv = bool::try_from_attr(v)?;
                    }
                    "preamble" => {
                        config.preamble = String::try_from_attr(v)?;
                    }
                    v => return Err(format!("unknown key {v:?} in gnuplot config")),
                }
            }
            Ok(config)
        }
    }

    /// Generate a gnuplot file that plots the timeseries data in the network
    #[network_func(timefmt = "%Y-%m-%d", skip_missing = false, config = GnuplotConfig::default())]
    fn plot_timeseries(
        net: &mut Network,
        csvfile: Template,
        datecol: &str,
        datacol: &str,
        outfile: &Path,
        timefmt: &str,
        config: &GnuplotConfig,
        skip_missing: bool,
    ) -> Result<(), String> {
        let mut plot_lines = Vec::with_capacity(net.nodes_count());
        for node in net.nodes() {
            let node = node.lock();
            let path = node.render(&csvfile).map_err(|e| e.to_string())?;
            if skip_missing && !PathBuf::from(&path).exists() {
                continue;
            }
            plot_lines.push(format!(
                "plot {path:?} using {datecol:?}:{datacol:?} with lines"
            ));
        }
	write_to_outfile(plot_lines, outfile, timefmt, config).map_err(|e| e.to_string())
    }
    fn write_to_outfile(plot_lines: Vec<String> ,outfile: &Path, timefmt: &str, config: &GnuplotConfig) -> Result<(), std::io::Error>{
        let nodes_count = plot_lines.len();

        let mut file = File::create(outfile)?;
        if config.csv {
            writeln!(file, "set datafile separator \",\"")?;
        }
        write!(
            file,
            "
set xdata time
set timefmt {timefmt:?}
unset key
set xtics format \"%Y\" rotate
set mxtics 6
set multiplot layout {nodes_count},1
"
        )?;
        if let Some(term) = &config.terminal {
            writeln!(file, "set terminal {term}")?;
        }
        if let Some(out) = &config.outfile {
            writeln!(file, "set output {out:?}")?;
        }
        writeln!(file, "{}", config.preamble)?;
        for line in plot_lines {
            writeln!(file, "{line}")?;
        }

        Ok(())
    }
}
