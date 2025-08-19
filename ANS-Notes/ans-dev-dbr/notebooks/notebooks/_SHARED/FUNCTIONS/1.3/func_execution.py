# Databricks notebook source
import networkx as nx
import matplotlib.pyplot as plt

# COMMAND ----------

def draw_graph(graph):
  plt.figure(figsize=(16,8))
  nx.draw_spectral(graph, with_labels = True)
  plt.show()

def get_graph_from_map(notebooks_map):
  graph = nx.DiGraph()
  
  for parent, children in notebooks_map.items():
    for child in children:
        graph.add_edge(parent.lower(), child.lower())  
  return graph

def get_child_nodes(graph, root_node):
  
  root_node = root_node.lower()
  
  result = {}
  for u, v in nx.bfs_edges(graph, root_node):
    level = len(max(nx.all_simple_paths(graph, root_node, v), key=lambda x: len(x))) - 1
    result[v] = level

  return [k for k, v in sorted(result.items(), key=lambda item: item[1], reverse=True)]

def get_notebooks(notebooks_map, root_node = 'root'):
  
  if not root_node:
    root_node = 'root'
  
  graph = get_graph_from_map(notebooks_map)
  notebooks = get_child_nodes(graph, root_node)
  
  if root_node.lower() != 'root':
    notebooks.append(root_node)
    
  notebooks = [n.upper() for n in notebooks]
  return notebooks

def run_notebook(notebook, timeout = 0, args = {}, max_retries = 2):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print("Retrying error", e)
        num_retries += 1
