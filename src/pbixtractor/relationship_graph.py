"""Relationship diagram (PNG) of the model, drawn with networkx + matplotlib."""

import re

import matplotlib
import networkx as nx
import pandas as pd

matplotlib.use("agg")
from matplotlib import pyplot as plt  # noqa: E402 (backend must be set first)


def save_relationship_graph(relations: pd.DataFrame, output_path: str) -> None:
    """
    Draw the relationships as a directed graph and save it as a PNG.

    Tables on the "one" side (Parent) are coloured and listed in the legend; the others
    (usually fact tables) are light green and labelled on the graph.

    Args:
        relations: Relationship rows with "Child" and "Parent" columns
        output_path: PNG file to write
    """
    width, height = 12, (len(relations) + 1) * 14.4 / 72
    graph = nx.DiGraph()

    for _, row in relations.iterrows():
        graph.add_node(row["Child"])
        if not pd.isnull(row["Parent"]):
            graph.add_edge(str(row["Parent"]), row["Child"])

    def split_label(label: str) -> str:
        return re.sub(r"([a-z])([A-Z])", r"\1\n\2", label)

    child_nodes = set(relations["Parent"].dropna().unique())
    parent_nodes = set(graph.nodes) - child_nodes

    colors = plt.cm.tab20.colors
    color_map = {node: colors[i % len(colors)] for i, node in enumerate(child_nodes)}
    node_colors = [color_map.get(node, "lightgreen") for node in graph.nodes]
    labels = {node: split_label(node) for node in parent_nodes}

    plt.figure(figsize=(width, height))
    positions = nx.spring_layout(graph, k=2.5, iterations=500, scale=10)
    nx.draw(
        graph,
        positions,
        with_labels=True,
        labels=labels,
        node_color=node_colors,
        font_weight="bold",
        node_size=300,
        arrowsize=10,
    )

    legend_handles = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=color_map[node],
            markersize=10,
            label=node,
        )
        for node in child_nodes
    ]
    plt.legend(
        handles=legend_handles, title="Dimensions", bbox_to_anchor=(1.05, 1), loc="upper left"
    )

    plt.savefig(output_path, bbox_inches="tight")
    plt.close()
