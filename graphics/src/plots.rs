use abi_stable::std_types::{RSome, Tuple2};
use nadi_core::prelude::*;
use nadi_core::table::ColumnAlign;
use nadi_core::table::Table;
use std::path::PathBuf;

#[derive(Debug)]
pub struct NetworkPlotConfig {
    pub width: f64,
    pub height: f64,
    pub delta_x: f64,
    pub delta_y: f64,
    pub offset: f64,
    pub radius: f64,
    pub fontsize: f64,
    pub fontface: cairo::FontFace,
}

impl Default for NetworkPlotConfig {
    fn default() -> Self {
        Self {
            width: 250.0,
            height: 300.0,
            delta_x: 20.0,
            delta_y: 20.0,
            offset: 30.0,
            radius: 3.0,
            fontsize: 16.0,
            fontface: cairo::FontFace::toy_create(
                "Serif",
                cairo::FontSlant::Normal,
                cairo::FontWeight::Normal,
            )
            .unwrap(),
        }
    }
}

impl FromAttributeRelaxed for NetworkPlotConfig {
    fn from_attr_relaxed(value: &Attribute) -> Option<Self> {
        Self::try_from_attr_relaxed(value).ok()
    }

    fn try_from_attr_relaxed(value: &Attribute) -> Result<Self, String> {
        let tab = AttrMap::try_from_attr(value)?;
        let mut config = Self::default();
        for Tuple2(k, v) in &tab {
            match k.as_str() {
                "width" => {
                    config.width = f64::try_from_attr_relaxed(v)?;
                }
                "height" => {
                    config.height = f64::try_from_attr_relaxed(v)?;
                }
                "delta_x" => {
                    config.delta_x = f64::try_from_attr_relaxed(v)?;
                }
                "delta_y" => {
                    config.delta_y = f64::try_from_attr_relaxed(v)?;
                }
                "offset" => {
                    config.offset = f64::try_from_attr_relaxed(v)?;
                }
                "radius" => {
                    config.radius = f64::try_from_attr_relaxed(v)?;
                }
                "fontsize" => {
                    config.fontsize = f64::try_from_attr_relaxed(v)?;
                }
                "fontface" => {
                    config.fontface = cairo::FontFace::toy_create(
                        &String::try_from_attr(v)?,
                        cairo::FontSlant::Normal,
                        cairo::FontWeight::Normal,
                    )
                    .map_err(|e| e.to_string())?;
                }
                v => return Err(format!("unknown key {v:?} in networkplot config")),
            }
        }
        Ok(config)
    }
}

// struct NetworkPlot {
//     nodes: usize,
//     levels: usize,
//     width: f64,
//     height: f64,
//     delx: f64,
//     dely: f64,
//     radius: f64,
// }

// impl NetworkPlotConfig {
//     fn fit_network(&mut self, net: &Network) -> anyhow::Result<()> {}
// }

/// Create a SVG file with the given network structure
pub fn export_svg_table(
    net: &mut Network,
    table: Table,
    outfile: PathBuf,
    config: NetworkPlotConfig,
    fit: bool,
    highlight: &[usize],
) -> anyhow::Result<()> {
    let n = net.nodes_count();
    if n == 0 {
        return Err(anyhow::Error::msg("Empty Network"));
    }
    let headers: Vec<&str> = table.columns.iter().map(|c| c.header.as_str()).collect();
    let contents: Vec<Vec<String>> = table
        .render_contents(&net, false)?
        .into_iter()
        .rev()
        .collect();

    let mut surf = cairo::SvgSurface::new::<&std::path::Path>(config.width, config.height, None)?;
    let ctx = cairo::Context::new(&mut surf)?;
    ctx.set_line_width(1.0);
    ctx.set_font_size(config.fontsize);
    ctx.set_font_face(&config.fontface);

    let header_widths: Vec<f64> = headers
        .iter()
        .map(|cell| {
            ctx.text_extents(cell)
                .map(|et| et.width())
                .unwrap_or_default()
        })
        .collect();
    let contents_widths: Vec<Vec<f64>> = contents
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    ctx.text_extents(cell)
                        .map(|et| et.width())
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect();
    let alignments: Vec<&ColumnAlign> = table.columns.iter().map(|c| &c.align).collect();

    let col_widths: Vec<f64> = header_widths
        .iter()
        .enumerate()
        .map(|(i, &h)| contents_widths.iter().map(|row| row[i]).fold(h, f64::max))
        .collect();

    let twidth: f64 =
        col_widths.iter().sum::<f64>() + config.offset * (col_widths.len() + 1) as f64;
    let mut delx = config.delta_x;
    let mut dely = config.delta_y;

    let max_level = net.nodes().map(|n| n.lock().level()).max().unwrap_or(0);

    let mut width = delx * max_level as f64 + 2.0 * config.radius + twidth;
    let mut height = dely * (n + 2) as f64 + 2.0 * config.radius;

    let mut surf = if fit {
        delx = (config.width - 2.0 * config.radius - twidth) / (max_level + 1) as f64;
        dely = (config.height - 2.0 * config.radius) / (n + 2) as f64;
        width = config.width;
        height = config.height;
        cairo::SvgSurface::new(config.width, config.height, Some(outfile))?
    } else {
        cairo::SvgSurface::new(width, height, Some(outfile))?
    };

    let ctx = cairo::Context::new(&mut surf)?;
    ctx.set_line_width(1.0);
    ctx.set_font_size(config.fontsize);
    ctx.set_font_face(&config.fontface);
    ctx.set_source_rgb(0.35, 0.35, 0.6);

    let offset = width - twidth;
    let col_stops: Vec<f64> = (0..(col_widths.len()))
        .map(|i| col_widths[0..i].iter().sum::<f64>() + config.offset * (i + 1) as f64 + offset)
        .collect();

    for (i, (head, a)) in headers.iter().zip(&alignments).enumerate() {
        let stop = match a {
            ColumnAlign::Left => col_stops[i],
            ColumnAlign::Right => col_stops[i] + col_widths[i] - header_widths[i],
            ColumnAlign::Center => col_stops[i] + (col_widths[i] - header_widths[i]) / 2.0,
        };
        ctx.move_to(stop, dely);
        ctx.show_text(head)?;
    }

    ctx.move_to(delx, dely * 1.5);
    ctx.line_to(width, dely * 1.5);
    ctx.stroke()?;

    net.nodes_rev()
        .zip(contents)
        .zip(contents_widths)
        .try_for_each(|((n, row), row_widths)| -> cairo::Result<()> {
            let n = n.lock();
            let y = height - (n.index() + 1) as f64 * dely;
            let x = n.level() as f64 * delx + config.offset / 2.0;

            ctx.set_source_rgb(0.35, 0.35, 0.6);
            if let RSome(o) = n.output() {
                let o = o.lock();
                let yo = height - (o.index() + 1) as f64 * dely;
                let xo = o.level() as f64 * delx + config.offset / 2.0;
                let dx = xo - x;
                let dy = yo - y;
                let l = (dx.powi(2) + dy.powi(2)).sqrt();
                let (ux, uy) = (dx / l, dy / l);
                let (sx, sy) = (x + ux * config.radius * 1.4, y + uy * config.radius * 1.4);
                let (ex, ey) = (xo - ux * config.radius * 1.4, yo - uy * config.radius * 1.4);
                ctx.move_to(sx, sy);
                ctx.line_to(ex, ey);
                ctx.stroke()?;
                let (asx, asy) = (ex - ux * config.radius, ey - uy * config.radius);
                let (aex, aey) = (xo - ux * config.radius, yo - uy * config.radius);
                ctx.move_to(
                    asx + uy * config.radius * 0.5,
                    asy - ux * config.radius * 0.5,
                );
                ctx.line_to(aex, aey);
                ctx.line_to(
                    asx - uy * config.radius * 0.5,
                    asy + ux * config.radius * 0.5,
                );
                ctx.line_to(asx + ux, asy + uy);
                ctx.fill()?;
                ctx.stroke()?;
            }
            if highlight.contains(&n.index()) {
                ctx.set_source_rgb(0.6, 0.35, 0.35);
            } else {
                ctx.set_source_rgb(0.35, 0.35, 0.6);
            }
            ctx.move_to(x + config.radius, y);
            ctx.arc(x, y, config.radius, 0.0, 2.0 * 3.1416);
            ctx.fill()?;
            ctx.stroke()?;

            for (i, (cell, a)) in row.iter().zip(&alignments).enumerate() {
                let stop = match a {
                    ColumnAlign::Left => col_stops[i],
                    ColumnAlign::Right => col_stops[i] + col_widths[i] - row_widths[i],
                    ColumnAlign::Center => col_stops[i] + (col_widths[i] - row_widths[i]) / 2.0,
                };
                ctx.move_to(stop, y);
                ctx.show_text(cell)?;
            }
            Ok(())
        })?;

    Ok(())
}

pub fn calc_text_width(
    texts: &[String],
    ctx: &cairo::Context,
    width: &mut f64,
) -> Result<bool, cairo::Error> {
    let mut changed = false;
    texts.iter().try_for_each(|n| {
        ctx.text_extents(n).map(|et| {
            if et.width() > *width {
                *width = et.width();
                changed = true;
            }
        })
    })?;
    Ok(changed)
}
