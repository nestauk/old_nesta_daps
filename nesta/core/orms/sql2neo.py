from py2neo import Node, Relationship, NodeSelector
from nesta.core.orms.orm_utils import object_to_dict
import logging


def table_to_neo4j(engine, transaction,
                   table=None, limit=None,
                   is_node=True, node_label=None,
                   rel_type=None):

    # Check if everything that's required has been provided
    assert table is not None, "No table was provided!"

    assert ((is_node is True and node_label is not None) or
            (is_node is False and rel_type is not None))

    graph = transaction.graph

    # Infer names of foreign key columns
    fks = []
    pks = []
    for col in table.columns:
        if col.foreign_keys:
            fks.append(col.name) # store the name of fk column of cooc table
            # for fk in col.foreign_keys: # store the name of id column
                # pks.append(fk.column.table.primary_key.column.name)

    # Check that we have only two columns with a foreign key
    assert(len(fk_table_names)==2)

    selector = NodeSelector(graph)

    # Query table, iterate through rows and send to neo4j
    for db, orm_instance in db_session_query(query=table, engine=engine,
                                             limit=limit, offset=irow):
        irow += 1
        if irow == limit:
            break

        if irow % 500 == 0:
            logging.info(f'Created {irow} new entities')

        data_row = object_to_dict(orm_instance, shallow=True)

        # If adding a node
        if is_node:
            graph.create(Node(node_label, **data_row))

        # If adding an edge
        else:
            first_node = selector.select(node_label, id=data_row[fk[0]]).first()
            second_node = selector.select(node_label, id=data_row[fk[1]]).first()

            data_row.pop(fk[0])
            data_row.pop(fk[1])

            rel = Relationship(first_node, rel_type, second_node, **data_row)
            graph.create(rel)

    return irow
