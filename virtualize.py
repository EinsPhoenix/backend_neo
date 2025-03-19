import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import json


def parse_neo4j_data(data_str):

    if isinstance(data_str, list):
        return data_str

    if isinstance(data_str, str):

        # Remove the first [ and last ] get individual arrays
        data_str = data_str.strip()
        if data_str.startswith("[[") and data_str.endswith("]]"):
            data_str = data_str[1:-1]  # Remove outer brackets

        # Split into separate JSON arrays
        json_arrays = []
        bracket_count = 0
        start_index = 0

        for i, char in enumerate(data_str):
            if char == "[":
                bracket_count += 1
                if bracket_count == 1:
                    start_index = i
            elif char == "]":
                bracket_count -= 1
                if bracket_count == 0:
                    json_arrays.append(data_str[start_index : i + 1])

        # Parse each JSON array
        parsed_data = []
        for json_array in json_arrays:
            try:
                parsed_data.append(json.loads(json_array))
            except json.JSONDecodeError:
                print(f"Failed to parse: {json_array}")

        return parsed_data

    print("Error: Data is not in a recognizable format")
    return []


def create_network_graph(parsed_data):
    G = nx.Graph()

    nodes = {}

    for relationship_set in parsed_data:
        for item in relationship_set:
            if item.get("type") == "node":
                node_id = item.get("id")
                if node_id not in nodes:
                    label = item.get("labels", ["Unknown"])[0]
                    properties = item.get("properties", {})
                    nodes[node_id] = {
                        "label": label,
                        "properties": properties,
                        "relationships": 0,
                    }
                    G.add_node(node_id, label=label, **properties)

            elif item.get("type") == "relationship":
                start_node = item.get("start", {}).get("id")
                end_node = item.get("end", {}).get("id")
                rel_label = item.get("label", "RELATED_TO")

                if start_node and end_node:
                    G.add_edge(start_node, end_node, label=rel_label)

                    if start_node in nodes:
                        nodes[start_node]["relationships"] += 1
                    if end_node in nodes:
                        nodes[end_node]["relationships"] += 1

    for node_id, node_data in nodes.items():
        G.nodes[node_id]["size"] = 100 + (node_data["relationships"] * 50)

    return G


def visualize_3d_graph(G):

    if len(G.nodes()) == 0:
        print("Error: Graph has no nodes to visualize")
        return

    pos_3d = nx.spring_layout(G, dim=3, seed=42)

    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111, projection="3d")

    node_colors = []
    node_sizes = []
    node_labels = {}

    color_map = {
        "UUID": "royalblue",
        "Color": "lime",
        "Temperature": "red",
        "Humidity": "cyan",
        "Timestamp": "gold",
        "EnergyCost": "purple",
        "EnergyConsume": "orange",
    }

    for node, attrs in G.nodes(data=True):
        label = attrs.get("label", "Unknown")
        node_colors.append(color_map.get(label, "gray"))
        node_sizes.append(attrs.get("size", 100))

        if "value" in attrs:
            node_labels[node] = f"{label}: {attrs['value']}"
        elif "id" in attrs:
            node_labels[node] = f"{label}: {attrs['id']}"
        else:
            node_labels[node] = f"{label}"

    xs = [pos_3d[node][0] for node in G.nodes()]
    ys = [pos_3d[node][1] for node in G.nodes()]
    zs = [pos_3d[node][2] for node in G.nodes()]

    ax.scatter(xs, ys, zs, c=node_colors, s=node_sizes, alpha=0.8, edgecolors="black")

    for edge in G.edges():
        x = [pos_3d[edge[0]][0], pos_3d[edge[1]][0]]
        y = [pos_3d[edge[0]][1], pos_3d[edge[1]][1]]
        z = [pos_3d[edge[0]][2], pos_3d[edge[1]][2]]
        ax.plot(x, y, z, "k-", alpha=0.4, linewidth=1)

    for node, (x, y, z) in pos_3d.items():
        if node in node_labels:
            ax.text(x, y, z, node_labels[node], fontsize=8)

    legend_elements = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            label=f"{label}",
            markerfacecolor=color,
            markersize=10,
        )
        for label, color in color_map.items()
    ]
    ax.legend(handles=legend_elements, loc="upper right")

    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_zlabel("Z")
    ax.set_title("3D Neo4j Graph Visualization")

    ax.grid(True)

    ax.set_box_aspect([1, 1, 1])

    plt.tight_layout()
    plt.show()


def load_neo4j_data(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            content = file.read()

            try:
                data = json.loads(content)
                return data
            except json.JSONDecodeError:

                return content
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        return None


# Example usage
def main():

    file_path = input("Enter the path to the Neo4j data file: ")

    if not file_path:
        file_path = r"./export_all_data.json"
        print(f"Using default path: {file_path}")

    raw_data = load_neo4j_data(file_path)

    if raw_data is None:
        print("Failed to load data. Exiting.")
        return

    parsed_data = parse_neo4j_data(raw_data)

    if not parsed_data:
        print("No valid data found after parsing. Exiting.")
        return

    G = create_network_graph(parsed_data)

    if len(G.nodes()) == 0:
        print("Graph has no nodes. Check your data format.")
        return

    visualize_3d_graph(G)


if __name__ == "__main__":
    main()
