use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod svg {
    use nadi_core::abi_stable::std_types::RSome;
    use nadi_core::graphics::node::{LINE_COLOR, LINE_WIDTH, NODE_COLOR, NODE_SIZE, TEXT_COLOR};
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use std::path::Path;
    use svg::node::element::*;
    use svg::Document;

    fn node_size(node: &NodeInner, size: &(&str, f64)) -> f32 {
        node.attr(size.0)
            .and_then(f64::from_attr_relaxed)
            .unwrap_or(size.1) as f32
    }

    fn node_color(node: &NodeInner, attr: &str) -> Option<String> {
        node.try_attr::<nadi_core::graphics::color::AttrColor>(attr)
            .unwrap_or_default()
            .color()
            .ok()
            .map(|c| {
                format!(
                    "#{:02X}{:02X}{:02X}",
                    (c.r * 255.0).round() as u64,
                    (c.g * 255.0).round() as u64,
                    (c.b * 255.0).round() as u64
                )
            })
    }

    /// Exports the network as a svg
    #[network_func(
	label = Template::parse_template("{_NAME}").unwrap(),
    )]
    fn svg_save(
        net: &mut Network,
        outfile: &Path,
        label: Template,
        width: Option<u64>,
        height: Option<u64>,
        bgcolor: Option<String>,
    ) -> anyhow::Result<()> {
        let count = net.nodes_count();
        let level = net
            .nodes()
            .map(|n| n.lock().level())
            .max()
            .unwrap_or_default();

        let mut nodes = Group::new();
        let mut edges = Group::new();
        for node in net.nodes() {
            let n = node.lock();
            let x = n.level() * 20 + 10;
            let y = (count - n.index()) * 20 + 10;
            let lab = n
                .render(&label)
                .unwrap_or_else(|_| label.original().to_string());
            let nd = Circle::new()
                .set("cx", x)
                .set("cy", y)
                .set("r", node_size(&n, &NODE_SIZE));
            let lab = Text::new(lab)
                .set("x", 20 * (level + 2))
                .set("y", y)
                .set("text-anchor", "start");
            nodes = nodes
                .add(match node_color(&n, NODE_COLOR.0) {
                    Some(c) => nd.set("fill", c),
                    None => nd,
                })
                .add(match node_color(&n, TEXT_COLOR.0) {
                    Some(c) => lab
                        .set("fill", c.clone())
                        .set("stroke", c)
                        .set("stroke-width", 0.5),
                    None => lab,
                });
            if let RSome(out) = n.output() {
                let o = out.lock();
                let xo = o.level() * 20 + 10;
                let yo = (count - o.index()) * 20 + 10;
                let l = Line::new()
                    .set("x1", x)
                    .set("y1", y)
                    .set("x2", xo)
                    .set("y2", yo)
                    .set("stroke-width", node_size(&n, &LINE_WIDTH));
                edges = edges.add(match node_color(&n, LINE_COLOR.0) {
                    Some(c) => l.set("stroke", c),
                    None => l,
                });
            }
        }
        let mut doc = Document::new().set(
            "viewBox",
            (
                0,
                0,
                width.unwrap_or(20 + 20 * level),
                height.unwrap_or(20 + 20 * count as u64),
            ),
        );
        if let Some(col) = bgcolor {
            doc = doc.add(
                Rectangle::new()
                    .set("height", "100%")
                    .set("width", "100%")
                    .set("fill", col),
            );
        }
        svg::save(outfile, &doc.add(edges).add(nodes))?;
        Ok(())
    }
}
