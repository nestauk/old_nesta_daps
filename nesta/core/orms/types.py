from sqlalchemy.dialects.mysql import VARCHAR as _VARCHAR
from sqlalchemy.dialects.mysql import TEXT as _TEXT
from functools import partial

TEXT = _TEXT(collation='utf8mb4_unicode_ci')
VARCHAR = partial(_VARCHAR, collation='utf8mb4_unicode_ci')
