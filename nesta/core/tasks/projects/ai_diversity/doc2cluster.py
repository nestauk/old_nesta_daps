"""
Transforms variable-length texts (usually from sentences
to paragraphs) to vectors using Sentence Transformers. Then, it fuzzy
clusters the vectors with HDBSCAN.
"""

from metaflow import FlowSpec, step, Parameter
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
import logging
from sentence_transformers import SentenceTransformer
import hdbscan
from nesta.core.luigihacks import misctools
from nesta.core.orms.arxiv_orm import Article, ArticleVector, ArticleCluster


class Doc2ClusterFlow(FlowSpec):
    """Fetches paper abtracts from a SQL database, transforms them to vectors
    and clusters them.
    """

    db_config = Parameter(
        "db_config", help="DB configuration filename", required=True, type=str
    )
    bert_model = Parameter(
        "transformer",
        help="""
        Name of the model used to encode documents.
        Full list: https://docs.google.com/spreadsheets/d/14QplCdTCDwEmTqrn1LH4yrbKvdogK4oQvYO1K1aPR5M/edit?usp=sharing
        """,
        required=True,
        type=str,
    )
    min_cluster_size = Parameter(
        "min_cluster_size",
        help="HDBSCAN: the smallest size grouping that you wish to consider a cluster",
        required=True,
        type=int,
    )
    min_samples = Parameter(
        "min_samples",
        help="""HDBSCAN: Measure of how conservative you want the clustering to be.
        The larger the value of min_samples you provide, the more conservative the clustering.""",
        required=True,
        type=int,
    )

    def _create_db_session(self, config, header="client", database="production"):
        """Creates a SQL engine.
        
        Args:
            config (str): Configuration filename.
            header (str): Header in the configuration file.
            database (str) Database name.

        Returns:
            (`sqlalchemy.orm.session.Session`)

        """
        db_config = misctools.get_config(config, header)
        url = URL(
            database=database,
            drivername="mysql+pymysql",
            username=db_config["user"],
            host=db_config["host"],
            password=db_config["password"],
            port=db_config["port"],
        )
        engine = create_engine(url)
        Session = sessionmaker(engine)
        return Session()

    def _create_mappings(self, ids, entities, column_name):
        """Transforms inputs to the bulk_insert_mappings() format.

        Args:
            ids (`list` of `str`): arXiv paper IDs.
            entities (`numpy.array` of `list` of `float`): Document
                vectors or cluster distribution.
            column_name (str): Name in corresponding table (ArticleVector or ArticleCluster).

        Returns:
            mapping (`list` of `dict`)

        """
        mapping = [
            {"article_id": id_, column_name: entity.astype(float),}
            for id_, entity in zip(ids, entities)
        ]
        return mapping

    @step
    def start(self):
        """Connects to SQL DB, filters vectorised articles and passes the rest
        to the next task.
        """
        # Connect to SQL DB
        s = self._create_db_session(self.db_config)

        # Get the abstracts and arXiv paper IDs.
        papers = s.query(Article.id, Article.abstract)
        logging.info(f"Number of documents to be vectorised: {papers.count()}")

        # Unroll abstracts and paper IDs
        self.ids, self.abstracts = zip(*papers)

        # Proceed to next task
        self.next(self.transform)

    @step
    def transform(self):
        """Transforms abstracts to vectors with Sentence Transformers."""
        # Instantiate SentenceTransformer
        model = SentenceTransformer(self.bert_model)

        # Convert text to vectors
        self.embeddings = model.encode(self.abstracts)

        # Proceed to next task
        self.next(self.cluster)

    @step
    def cluster(self):
        """Soft clustering with HDBSCAN."""
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=self.min_cluster_size,
            min_samples=self.min_samples,
            prediction_data=True,
        ).fit(self.embeddings)
        self.soft_clusters = hdbscan.all_points_membership_vectors(clusterer)
        self.next(self.end)

    @step
    def end(self):
        """Creates mappings and stores them in SQL DB."""
        # Group arXiv paper IDs with embeddings
        id_embeddings_mapping = self._create_mappings(
            self.ids, self.embeddings, "vector"
        )
        # Group arXiv paper IDs with clusters
        id_clusters_mapping = self._create_mappings(
            self.ids, self.soft_clusters, "clusters"
        )

        # Connect to SQL DB
        s = self._create_db_session(self.db_config)
        # Store to db
        s.bulk_insert_mappings(ArticleVector, id_embeddings_mapping)
        s.bulk_insert_mappings(ArticleCluster, id_clusters_mapping)
        s.commit()


if __name__ == "__main__":
    Doc2ClusterFlow()
