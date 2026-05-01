"""uq_verification_jobs_workflow_id

Revision ID: 2026_01_27_10:16:24
Revises: 2025_04_24_18:29:11
Create Date: 2026-01-27 10:16:24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel



# revision identifiers, used by Alembic.
revision: str = '2026_01_27_10:16:24'
down_revision: Union[str, None] = '2025_04_24_18:29:11'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add unique constraint on workflow_id to prevent duplicate verification jobs per workflow
    op.create_unique_constraint('uq_verification_jobs_workflow_id', 'verification_jobs', ['workflow_id'])


def downgrade() -> None:
    op.drop_constraint('uq_verification_jobs_workflow_id', 'verification_jobs', type_='unique')
