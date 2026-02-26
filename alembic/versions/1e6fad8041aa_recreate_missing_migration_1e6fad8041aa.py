"""recreate missing migration 1e6fad8041aa"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = '1e6fad8041aa'
down_revision = None
branch_labels = None
depends_on = None

def _table_exists(table_name):
    bind = op.get_bind()
    inspector = inspect(bind)
    return table_name in inspector.get_table_names()

def upgrade():
    # No-op seguro por padrão.
    pass

def downgrade():
    # No-op seguro.
    pass
