use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod print_node {
    use abi_stable::std_types::RSome;
    use nadi_core::nadi_plugin::{network_func, node_func};
    use nadi_core::prelude::*;

    /// Print the node with its inputs and outputs
    #[node_func]
    fn print_node(node: &mut NodeInner) {
        let inputs: String = node
            .inputs()
            .iter()
            .map(|i| i.lock().name().to_string())
            .collect::<Vec<String>>()
            .join(",");
        if !inputs.is_empty() {
            print!("{{ {} }} -> ", inputs);
        }
        print!("{}", node.name());
        if let RSome(out) = node.output() {
            println!(" -> {}", out.lock().name())
        } else {
            println!()
        }
    }

    /// Print the given attributes in csv format with first column with node name
    #[network_func]
    fn print_attr_csv(net: &mut Network, #[args] args: &[Attribute]) -> Result<(), String> {
        let attrs_n = args
            .iter()
            .map(String::try_from_attr)
            .collect::<Result<Vec<String>, String>>()?;
        println!("name,{}", attrs_n.join(","));
        for node in net.nodes() {
            let node = node.lock();
            let attrs: Vec<String> = attrs_n
                .iter()
                .map(|a| node.attr(a).map(|v| v.to_string()).unwrap_or_default())
                .collect();
            println!("{},{}", node.name(), attrs.join(","));
        }
        Ok(())
    }
}
