from graphviz import Digraph

graph_array=['Step A->Step B->Step C', 'Step A->Step D', 'Step D->Step B', 'Step B -> Step N -> Step A']
graph_title='[Data Flow Diagram]'

from graphviz import Digraph

def create_flow_diagram(sequences, label):
    # Create a new directed graph
    dot = Digraph()
    dot.attr(splines='true', rankdir='TB')
    dot.attr('node', shape='plaintext', fontname='Helvetica', fontsize='11',
             fontcolor='black', style='filled', fillcolor='#e9e9e9', width='1.5')
    dot.attr('edge', arrowhead='normal', arrowtail='dot', color='#20B2AA', style='solid')

    # Graph title
    dot.attr(labelloc='t', labeljust='c', 
            fontcolor='#20B2AA', fontname='Courier New Bold', fontsize='20')
    dot.attr(label=label)

    # Get all unique steps
    steps = set(step.strip() for sequence in sequences for step in sequence.split('->'))

    # Add each step as a node to the graph
    for step in steps:
        dot.node(step, label=f'► {step}')

    # Add edges between the steps for each sequence
    for sequence in sequences:
        sequence_steps = [step.strip() for step in sequence.split('->')]
        for i in range(len(sequence_steps) - 1):
            dot.edge(sequence_steps[i], sequence_steps[i+1])

    # Save the graph to a file
    dot.render('flow_diagram.gv', view=True)

# Use the function
create_flow_diagram(graph_array, graph_title)
