"""
Doc2Cluster
===========
Fetches articles from DB and transforms variable-length texts (usually from sentences to paragraphs)
to vectors using Sentence Transformers. Fuzzy clusters the vectors with HDBSCAN.

Tasks:
- start: Fetches articles without a vector from DB.
- transform: Encodes abstracts as vectors and stores them in DB.
- cluster: Applies soft clustering with HDBSCAN. It can either predict a cluster distribution for 
    the newly encoded abstracts using a pre-fitted model or fitting a model on the whole corpus. 
    The model is stored/loaded from S3. When fitting a new model, it deletes the previous cluster 
    predictions from the DB.
- end: Exit metaflow pipeline.

The pipeline takes a set of parameters. Two examples:
1. Run the pipeline and fit a new HDBSCAN model
python doc2cluster.py --no-pylint run --transformer distilbert-base-nli-stsb-mean-tokens --db_config mysqldb.config --min_cluster_size 10 --min_samples 2 --new_clusterer True --clusterer_name hdbscan_model --s3_bucket test-document-vectors

2. Run the pipeline and predict a cluster distribution for vectors using a fitted HDBSCAN model 
python doc2cluster.py --no-pylint run --transformer distilbert-base-nli-stsb-mean-tokens --db_config mysqldb.config --new_clusterer False --clusterer_name hdbscan_model --s3_bucket test-document-vectors
"""

from metaflow import FlowSpec, step, Parameter
from sqlalchemy import create_engine
from sqlalchemy.sql import exists
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
import logging
from sentence_transformers import SentenceTransformer
import hdbscan
import numpy as np
from nesta.core.luigihacks import misctools
from nesta.core.orms.arxiv_orm import Article, ArticleVector, ArticleCluster
from nesta.core.orms.arxiv_orm import Base
from nesta.packages.misc_utils.s3_utils import pickle_to_s3, s3_to_pickle
from nesta.core.orms.orm_utils import db_session


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
    new_clusterer = Parameter(
        "new_clusterer",
        help="Fits a new cluster model using all the document vectors. If set to False, it will fetch a fitted cluster model from S3.",
        required=True,
        type=bool,
    )
    clusterer_name = Parameter(
        "clusterer_name", help="Filename of the clusterer.", required=True, type=str
    )
    min_cluster_size = Parameter(
        "min_cluster_size",
        help="HDBSCAN: the smallest size grouping that you wish to consider a cluster. Required only when new_clusterer=True.",
        required=False,
        type=int,
    )
    min_samples = Parameter(
        "min_samples",
        help="""HDBSCAN: Measure of how conservative you want the clustering to be.
        The larger the value of min_samples you provide, the more conservative the clustering. Required only when new_clusterer=True.""",
        required=False,
        type=int,
    )
    s3_bucket = Parameter(
        "s3_bucket",
        help="S3 bucket to store/load the clusterer.",
        required=True,
        type=str,
    )

    def _create_engine(self, config, header="client", database="production"):
        """Creates a SQL engine.

        Args:
            config (str): Configuration filename.
            header (str): Header in the configuration file.
            database (str) Database name.

        Returns:
            (`sqlalchemy.engine.base.Engine`)

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
        return create_engine(url)

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
        # Connect to SQL DB and get the abstracts and arXiv paper IDs.
        engine = self._create_engine(self.db_config)
        Base.metadata.create_all(engine)  # Required for first-time use
        with db_session(engine) as session:
            papers = session.query(Article.id, Article.abstract).filter(
                ~exists().where(Article.id == ArticleVector.article_id)
            )

        # Unroll abstracts and paper IDs
        self.ids, self.abstracts = zip(*papers)

        # Proceed to next task
        self.next(self.transform)

    @step
    def transform(self):
        """Transforms abstracts to vectors with Sentence Transformers."""
        logging.info(f"Number of documents to be vectorised: {len(self.ids)}")

        # Instantiate SentenceTransformer
        model = SentenceTransformer(self.bert_model)

        # Convert text to vectors
        self.embeddings = model.encode(self.abstracts)

        # Group arXiv paper IDs with embeddings
        id_embeddings_mapping = self._create_mappings(
            self.ids, self.embeddings, "vector"
        )
        # Connect to SQL DB and store vectors and arXiv paper IDs
        engine = self._create_engine(self.db_config)
        with db_session(engine) as session:
            session.bulk_insert_mappings(ArticleVector, id_embeddings_mapping)
            logging.info("Committed vectors to DB!")

        # Proceed to next task
        self.next(self.cluster)

    @step
    def cluster(self):
        """Soft clustering with HDBSCAN."""
        if self.new_clusterer:
            logging.info("Fitting a new HDBSCAN model.")
            # Create SQL engine
            engine = self._create_engine(self.db_config)
            with db_session(engine) as session:
                # Delete all clusters to refill the table with the new predictions.
                session.query(ArticleCluster).delete()

                # Fetch all document embeddings
                papers = session.query(ArticleVector.article_id, ArticleVector.vector)

            # Unroll abstracts and paper IDs
            self.ids, self.embeddings = zip(*papers)

            # Fit HDBSCAN
            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=self.min_cluster_size,
                min_samples=self.min_samples,
                prediction_data=True,
            ).fit(self.embeddings)

            # Assign soft clusters to embeddings
            self.soft_clusters = hdbscan.all_points_membership_vectors(clusterer)

            # Store clusterer in S3
            pickle_to_s3(clusterer, self.s3_bucket, self.clusterer_name)
        else:
            logging.info("Loading fitted HDBSCAN from S3.")
            # Load clusterer from S3
            clusterer = s3_to_pickle(self.s3_bucket, self.clusterer_name)

            # Predict soft labels
            self.soft_clusters = hdbscan.prediction.membership_vector(
                clusterer, np.array(self.embeddings)
            )

        # Group arXiv paper IDs with clusters
        id_clusters_mapping = self._create_mappings(
            self.ids, self.soft_clusters, "clusters"
        )
        # Create SQL engine
        engine = self._create_engine(self.db_config)
        with db_session(engine) as session:
            # Store mapping in DB
            session.bulk_insert_mappings(ArticleCluster, id_clusters_mapping)
        self.next(self.end)

    @step
    def end(self):
        """Gracefully exit metaflow."""
        logging.info("Tasks completed.")


if __name__ == "__main__":
    Doc2ClusterFlow()
