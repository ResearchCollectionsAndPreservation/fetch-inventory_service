"""add_can_manage_users_permission

Revision ID: 2026_02_09_12:00:00
Revises: 2026_01_27_10:16:24
Create Date: 2026-02-09 12:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2026_02_09_12:00:00'
down_revision: Union[str, None] = '2026_01_27_10:16:24'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Insert the new permission
    op.execute("""
        INSERT INTO permissions (name, description, create_dt, update_dt)
        VALUES ('can_manage_users', 'Allows creating, updating, and deleting users', NOW(), NOW())
        ON CONFLICT (name) DO NOTHING
    """)

    # Add permission to admin group
    op.execute("""
        INSERT INTO group_permissions (group_id, permission_id)
        SELECT g.id, p.id
        FROM groups g, permissions p
        WHERE g.name = 'admin' AND p.name = 'can_manage_users'
        ON CONFLICT ON CONSTRAINT uq_permission_id_group_id DO NOTHING
    """)


def downgrade() -> None:
    # Remove permission from admin group
    op.execute("""
        DELETE FROM group_permissions
        WHERE permission_id = (SELECT id FROM permissions WHERE name = 'can_manage_users')
    """)

    # Delete the permission
    op.execute("""
        DELETE FROM permissions WHERE name = 'can_manage_users'
    """)
