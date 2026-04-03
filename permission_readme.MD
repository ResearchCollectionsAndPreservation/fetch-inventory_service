# API Endpoint Permissions Reference

This document lists every API endpoint and the permission(s) required to access it.

- **None (open)** = no `require_permissions` guard; accessible to any authenticated user
- **any_of** = user needs at least ONE of the listed permissions

---

## Open Endpoints (No Permission Required)

The following endpoints have no `require_permissions` guard. They are accessible to any authenticated user.

### Intentionally Open

| Method | Path | Rationale |
|--------|------|-----------|
| GET | `/status/` | Health check |
| GET | `/auth/sso/metadata` | SAML metadata (pre-auth) |
| GET | `/auth/sso/login` | SAML login (pre-auth) |
| POST | `/auth/sso/acs` | SAML callback (pre-auth) |
| POST | `/auth/legacy/login` | Dev/test login (pre-auth) |
| GET | `/profile/` | Query profiler |

### Read-Only Reference Data — Used by Frontend

| Method | Path | Resource |
|--------|------|----------|
| GET | `/sides/` | Sides |
| GET | `/sides/{id}` | Sides |
| GET | `/owners/tiers` | Owner tiers |
| GET | `/owners/tiers/{id}` | Owner tiers |
| GET | `/barcodes/` | Barcodes |
| GET | `/barcodes/{id}` | Barcodes |
| GET | `/barcodes/value/{value}` | Barcodes |

### Read-Only Reference Data — Not Used by Frontend

| Method | Path | Resource |
|--------|------|----------|
| GET | `/ladders/numbers` | Ladder numbers |
| GET | `/ladders/numbers/{id}` | Ladder numbers |
| GET | `/shelves/numbers` | Shelf numbers |
| GET | `/shelves/numbers/{id}` | Shelf numbers |
| GET | `/shelves/positions/numbers` | Shelf position numbers |
| GET | `/shelves/positions/numbers/{id}` | Shelf position numbers |
| GET | `/sides/orientations` | Side orientations |
| GET | `/sides/orientations/{id}` | Side orientations |
| GET | `/barcodes/types` | Barcode types |
| GET | `/barcodes/types/{id}` | Barcode types |
| GET | `/conveyance-bins/` | Conveyance bins |
| GET | `/conveyance-bins/{id}` | Conveyance bins |
| GET | `/subcollections/` | Subcollections |
| GET | `/subcollections/{id}` | Subcollections |
| GET | `/item-retrieval-events/` | Item retrieval events |
| GET | `/item-retrieval-events/{id}` | Item retrieval events |
| GET | `/non-tray-item-retrieval-events/` | Non-tray item retrieval events |
| GET | `/non-tray-item-retrieval-events/{id}` | Non-tray item retrieval events |

### Read-Only User Data

| Method | Path | Resource |
|--------|------|----------|
| GET | `/users/` | User list |
| GET | `/users/{id}` | User detail |
| GET | `/users/{id}/groups` | User groups |
| GET | `/users/{user_id}/permissions` | User permissions |

---

## All Endpoints by Resource

## Accession Jobs

**Prefix:** `/accession-jobs`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/accession-jobs/` | get_accession_job_list | can_access_accession |
| GET | `/accession-jobs/{id}` | get_accession_job_detail | can_access_accession |
| GET | `/accession-jobs/workflow/{id}` | get_accession_job_detail_by_workflow | can_access_accession |
| POST | `/accession-jobs/` | create_accession_job | can_access_accession |
| PATCH | `/accession-jobs/{id}` | update_accession_job | can_access_accession |
| DELETE | `/accession-jobs/{id}` | delete_accession_job | can_cancel_accession |

## Aisle Numbers

**Prefix:** `/aisles`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/aisles/numbers` | get_aisle_number_list | can_manage_locations |
| GET | `/aisles/numbers/{id}` | get_aisle_number_detail | can_manage_locations |
| POST | `/aisles/numbers` | create_aisle_number | can_manage_locations |
| PATCH | `/aisles/numbers/{id}` | update_aisle_number | can_manage_locations |
| DELETE | `/aisles/numbers/{id}` | delete_aisle_number | can_manage_locations |

## Aisles

**Prefix:** `/aisles`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/aisles/` | get_aisle_list | can_manage_locations |
| GET | `/aisles/{id}` | get_aisle_detail | can_manage_locations |
| POST | `/aisles/` | create_aisle | can_manage_locations |
| PATCH | `/aisles/{id}` | update_aisle | can_manage_locations |
| DELETE | `/aisles/{id}` | delete_aisle | can_manage_locations |

## Audit Trails

**Prefix:** `/history`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/history/` | get_audit_trails_list | can_access_reports |
| GET | `/history/{table_name}/{record_id}` | get_audit_trails_detail_list | can_access_reports |

## Auth

**Prefix:** `/auth`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/auth/sso/metadata` | saml_metadata | None (open) |
| GET | `/auth/sso/login` | saml_login | None (open) |
| POST | `/auth/sso/acs` | saml_acs | None (open) |
| POST | `/auth/legacy/login` | legacy_login | None (open, dev/test only) |

## Barcode Types

**Prefix:** `/barcodes`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/barcodes/types` | get_barcode_types_list | None (open) |
| GET | `/barcodes/types/{id}` | get_barcode_types_detail | None (open) |
| POST | `/barcodes/types` | create_barcode_types | can_manage_locations |
| PATCH | `/barcodes/types/{id}` | update_barcode_types | can_manage_locations |
| DELETE | `/barcodes/types/{id}` | delete_barcode_types | can_manage_locations |

## Barcodes

**Prefix:** `/barcodes`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/barcodes/` | get_barcode_list | None (open) |
| GET | `/barcodes/{id}` | get_barcode_detail | None (open) |
| GET | `/barcodes/value/{value}` | get_barcode_by_value | None (open) |
| POST | `/barcodes/` | create_barcode | can_access_accession OR can_access_verification (any_of) |
| PATCH | `/barcodes/{id}` | update_barcode | can_access_accession OR can_access_verification (any_of) |
| DELETE | `/barcodes/{id}` | delete_barcode | can_access_accession OR can_access_verification (any_of) |

## Batch Upload

**Prefix:** `/batch-upload`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/batch-upload/` | get_batch_upload | can_create_and_submit_batch_requests |
| GET | `/batch-upload/{id}` | get_batch_upload_detail | can_create_and_submit_batch_requests |
| DELETE | `/batch-upload/{id}` | delete_batch_upload | can_create_and_submit_batch_requests |
| PATCH | `/batch-upload/{id}` | update_batch_upload | can_create_and_submit_batch_requests |
| POST | `/batch-upload/request` | batch_upload_request | can_create_and_submit_batch_requests |
| POST | `/batch-upload/withdraw-jobs/{job_id}` | batch_upload_withdraw_job | can_create_and_submit_batch_requests |
| POST | `/batch-upload/location-management` | batch_upload_location_management | can_create_and_submit_batch_requests |

## Buildings

**Prefix:** `/buildings`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/buildings/` | get_building_list | can_manage_locations |
| GET | `/buildings/{id}` | get_building_detail | can_manage_locations |
| POST | `/buildings/` | create_building | can_manage_locations |
| PATCH | `/buildings/{id}` | update_building | can_manage_locations |
| DELETE | `/buildings/{id}` | delete_building | can_manage_locations |

## Container Types

**Prefix:** `/container-types`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/container-types/` | get_container_type_list | can_manage_locations |
| GET | `/container-types/{id}` | get_container_type_detail | can_manage_locations |
| POST | `/container-types/` | create_container_type | can_manage_locations |
| PATCH | `/container-types/{id}` | update_container_type | can_manage_locations |
| DELETE | `/container-types/{id}` | delete_container_type | can_manage_locations |

## Conveyance Bins

**Prefix:** `/conveyance-bins`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/conveyance-bins/` | get_conveyance_bin_list | None (open) |
| GET | `/conveyance-bins/{id}` | get_conveyance_bin_detail | None (open) |
| POST | `/conveyance-bins/` | create_conveyance_bin | can_manage_locations |
| PATCH | `/conveyance-bins/{id}` | update_conveyance_bin | can_manage_locations |
| DELETE | `/conveyance-bins/{id}` | delete_conveyance_bin | can_manage_locations |

## Delivery Locations

**Prefix:** `/requests`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/requests/locations` | get_delivery_location_list | can_access_request OR can_search (any_of) |
| GET | `/requests/locations/{id}` | get_delivery_location_detail | can_access_request OR can_search (any_of) |
| POST | `/requests/locations` | create_delivery_location | can_manage_groups_and_permissions |
| PATCH | `/requests/locations/{id}` | update_delivery_location | can_manage_groups_and_permissions |
| DELETE | `/requests/locations/{id}` | delete_delivery_location | can_manage_groups_and_permissions |

## Groups

**Prefix:** `/groups`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/groups/` | get_group_list | can_manage_groups_and_permissions |
| GET | `/groups/{id}` | get_group_detail | can_manage_groups_and_permissions |
| POST | `/groups/` | create_group | can_manage_groups_and_permissions |
| PATCH | `/groups/{id}` | update_group | can_manage_groups_and_permissions |
| DELETE | `/groups/{id}` | delete_group | can_manage_groups_and_permissions |
| GET | `/groups/{id}/users` | get_group_users | can_manage_groups_and_permissions |
| POST | `/groups/{group_id}/add_user/{user_id}` | add_user_to_group | can_manage_groups_and_permissions |
| DELETE | `/groups/{group_id}/remove_user/{user_id}` | remove_user_from_group | can_manage_groups_and_permissions |
| GET | `/groups/{group_id}/permissions` | get_group_permissions | can_manage_groups_and_permissions |
| POST | `/groups/{group_id}/add_permission/{permission_id}` | add_permission_to_group | can_manage_groups_and_permissions |
| DELETE | `/groups/{group_id}/remove_permission/{permission_id}` | remove_permission_from_group | can_manage_groups_and_permissions |

## Item Retrieval Events

**Prefix:** `/item-retrieval-events`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/item-retrieval-events/` | get_item_retrieval_events | None (open) |
| GET | `/item-retrieval-events/{id}` | get_item_retrieval_event_detail | None (open) |
| POST | `/item-retrieval-events/` | create_item_retrieval_event | can_manage_locations |
| PATCH | `/item-retrieval-events/{id}` | update_item_retrieval_event | can_manage_locations |
| DELETE | `/item-retrieval-events/{id}` | delete_item_retrieval_event | can_manage_locations |

## Items

**Prefix:** `/items`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/items/` | get_item_list | can_access_item_detail |
| GET | `/items/download` | download_items | can_access_item_detail |
| GET | `/items/{id}` | get_item_detail | can_access_item_detail |
| GET | `/items/barcode/{value}` | get_item_by_barcode_value | can_access_item_detail |
| POST | `/items/` | create_item | can_access_accession |
| PATCH | `/items/{id}` | update_item | can_access_accession OR can_access_verification (any_of) |
| DELETE | `/items/{id}` | delete_item | can_access_accession |
| POST | `/items/move/{barcode_value}` | move_item | can_move_trays_and_items |

## Ladder Numbers

**Prefix:** `/ladders`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/ladders/numbers` | get_ladder_number_list | None (open) |
| GET | `/ladders/numbers/{id}` | get_ladder_number_detail | None (open) |
| POST | `/ladders/numbers` | create_ladder_number | can_manage_locations |
| PATCH | `/ladders/numbers/{id}` | update_ladder_number | can_manage_locations |
| DELETE | `/ladders/numbers/{id}` | delete_ladder_number | can_manage_locations |

## Ladders

**Prefix:** `/ladders`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/ladders/` | get_ladder_list | can_manage_locations |
| GET | `/ladders/{id}` | get_ladder_detail | can_manage_locations |
| POST | `/ladders/` | create_ladder | can_manage_locations |
| PATCH | `/ladders/{id}` | update_ladder | can_manage_locations |
| DELETE | `/ladders/{id}` | delete_ladder | can_manage_locations |

## Media Types

**Prefix:** `/media-types`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/media-types/` | get_media_type_list | can_manage_media_type |
| GET | `/media-types/{id}` | get_media_type_detail | can_manage_media_type |
| POST | `/media-types/` | create_media_type | can_manage_media_type |
| PATCH | `/media-types/{id}` | update_media_type | can_manage_media_type |
| DELETE | `/media-types/{id}` | delete_media_type | can_manage_media_type |

## Modules

**Prefix:** `/modules`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/modules/` | get_module_list | can_manage_locations |
| GET | `/modules/{id}` | get_module_detail | can_manage_locations |
| POST | `/modules/` | create_module | can_manage_locations |
| PATCH | `/modules/{id}` | update_module | can_manage_locations |
| DELETE | `/modules/{id}` | delete_module | can_manage_locations |

## Non-Tray Item Retrieval Events

**Prefix:** `/non-tray-item-retrieval-events`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/non-tray-item-retrieval-events/` | get_non_tray_item_retrieval_events | None (open) |
| GET | `/non-tray-item-retrieval-events/{id}` | get_non_tray_item_retrieval_event_detail | None (open) |
| POST | `/non-tray-item-retrieval-events/` | create_non_tray_item_retrieval_event | can_manage_locations |
| PATCH | `/non-tray-item-retrieval-events/{id}` | update_non_tray_item_retrieval_event | can_manage_locations |
| DELETE | `/non-tray-item-retrieval-events/{id}` | delete_non_tray_item_retrieval_event | can_manage_locations |

## Non-Tray Items

**Prefix:** `/non_tray_items`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/non_tray_items/` | get_non_tray_item_list | can_access_item_detail |
| GET | `/non_tray_items/download` | download_non_tray_items | can_access_item_detail |
| GET | `/non_tray_items/{id}` | get_non_tray_item_detail | can_access_item_detail |
| GET | `/non_tray_items/barcode/{value}` | get_non_tray_by_barcode_value | can_access_item_detail |
| POST | `/non_tray_items/` | create_non_tray_item | can_access_accession |
| PATCH | `/non_tray_items/{id}` | update_non_tray_item | can_access_accession OR can_access_verification (any_of) |
| DELETE | `/non_tray_items/{id}` | delete_non_tray_item | can_access_accession |
| POST | `/non_tray_items/move/{barcode_value}` | move_item | can_move_trays_and_items |

## Owner Tiers

**Prefix:** `/owners`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/owners/tiers` | get_owner_tier_list | None (open) |
| GET | `/owners/tiers/{id}` | get_owner_tier_detail | None (open) |
| POST | `/owners/tiers` | create_owner_tier | can_manage_owners |
| PATCH | `/owners/tiers/{id}` | update_owner_tier | can_manage_owners |
| DELETE | `/owners/tiers/{id}` | delete_owner_tier | can_manage_owners |

## Owners

**Prefix:** `/owners`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/owners/` | get_owner_list | can_manage_owners |
| GET | `/owners/{id}` | get_owner_detail | can_manage_owners |
| POST | `/owners/` | create_owner | can_manage_owners |
| PATCH | `/owners/{id}` | update_owner | can_manage_owners |
| DELETE | `/owners/{id}` | delete_owner | can_manage_owners |

## Permissions

**Prefix:** `/permissions`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/permissions/` | get_permission_list | can_manage_groups_and_permissions |
| GET | `/permissions/{id}` | get_permission_detail | can_manage_groups_and_permissions |
| POST | `/permissions/` | create_permission | can_manage_groups_and_permissions |
| PATCH | `/permissions/{id}` | update_permission | can_manage_groups_and_permissions |
| DELETE | `/permissions/{id}` | delete_permission | can_manage_groups_and_permissions |

## Pick Lists

**Prefix:** `/pick-lists`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/pick-lists/` | get_pick_list_list | can_access_picklist |
| GET | `/pick-lists/{id}` | get_pick_list_detail | can_access_picklist |
| POST | `/pick-lists/` | create_pick_list | can_create_picklist_job |
| PATCH | `/pick-lists/{id}` | update_pick_list | can_edit_picklist_job |
| PATCH | `/pick-lists/{pick_list_id}/add_request` | add_request_to_pick_list | can_add_to_picklist_job |
| PATCH | `/pick-lists/{pick_list_id}/update_request/{request_id}` | update_request_for_pick_list | can_edit_picklist_job |
| DELETE | `/pick-lists/{pick_list_id}/remove_request/{request_id}` | remove_request_from_pick_list | can_edit_picklist_job |
| DELETE | `/pick-lists/{id}` | delete_pick_list | can_delete_picklist_job |

## Priorities

**Prefix:** `/requests`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/requests/priorities` | get_priority_list | can_access_request OR can_search (any_of) |
| GET | `/requests/priorities/{id}` | get_priority_detail | can_access_request OR can_search (any_of) |
| POST | `/requests/priorities` | create_priority | can_manage_groups_and_permissions |
| PATCH | `/requests/priorities/{id}` | update_priority | can_manage_groups_and_permissions |
| DELETE | `/requests/priorities/{id}` | delete_priority | can_manage_groups_and_permissions |

## Query Profiler

**Prefix:** `/profile`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/profile/` | profile_report | None (open) |

## Refile Jobs

**Prefix:** `/refile-jobs`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/refile-jobs/` | get_refile_job_list | can_access_refile |
| GET | `/refile-jobs/{id}` | get_refile_job_detail | can_access_refile |
| POST | `/refile-jobs/` | create_refile_job | can_create_refile_job |
| PATCH | `/refile-jobs/{id}` | update_refile_job | can_edit_refile_job |
| DELETE | `/refile-jobs/{id}` | delete_refile_job | can_delete_refile_job |
| POST | `/refile-jobs/{job_id}/add_items` | add_items_to_refile_job | can_add_to_refile_job |
| DELETE | `/refile-jobs/{job_id}/remove_items` | remove_item_from_refile_job | can_edit_refile_job |
| PATCH | `/refile-jobs/{job_id}/update_item/{item_id}` | update_item_in_refile_job | can_edit_refile_job |
| PATCH | `/refile-jobs/{job_id}/update_non_tray_items/{non_tray_item_id}` | update_non_tray_item_in_refile_job | can_edit_refile_job |

## Refile Queue

**Prefix:** `/refile-queue`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/refile-queue/` | get_refile_queue_list | can_access_refile |
| PATCH | `/refile-queue/` | add_to_refile_queue | can_add_refile_item_to_queue |
| DELETE | `/refile-queue/` | remove_from_refile_queue | can_add_refile_item_to_queue |

## Reporting

**Prefix:** `/reporting`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/reporting/accession-items/` | get_accessioned_items_count | can_access_reports |
| GET | `/reporting/accession-items/download` | get_accessioned_items_csv | can_access_reports |
| GET | `/reporting/shelving-job-discrepancies/` | get_shelving_job_discrepancy_list | can_access_reports |
| GET | `/reporting/shelving-job-discrepancies/download` | get_shelving_job_report_csv | can_access_reports |
| GET | `/reporting/shelving-job-discrepancies/{id}` | get_shelving_job_discrepancy_detail | can_access_reports |
| GET | `/reporting/open-locations/` | get_open_locations_list | can_access_reports |
| GET | `/reporting/open-locations/download` | get_open_locations_csv | can_access_reports |
| GET | `/reporting/aisles/items_count/` | get_aisle_items_count | can_access_reports |
| GET | `/reporting/aisles/items_count/download` | get_aisles_items_count_csv | can_access_reports |
| GET | `/reporting/non_tray_items/count/` | get_non_tray_item_count | can_access_reports |
| GET | `/reporting/non_tray_items/count/download` | get_non_tray_item_count_csv | can_access_reports |
| GET | `/reporting/tray_items/count/` | get_tray_item_count | can_access_reports |
| GET | `/reporting/tray_items/count/download` | get_tray_item_count_csv | can_access_reports |
| GET | `/reporting/user-jobs/count/` | get_user_job_summary | can_access_reports |
| GET | `/reporting/user-jobs/count/download` | get_user_job_summary_csv | can_access_reports |
| GET | `/reporting/verification-changes/summary/` | get_verification_change_summary | can_access_reports |
| GET | `/reporting/verification-changes/summary/download` | get_verification_change_summary_csv | can_access_reports |
| GET | `/reporting/retrievals/count/` | get_retrieval_count | can_access_reports |
| GET | `/reporting/retrievals/count/download` | get_retrieval_count_csv | can_access_reports |
| GET | `/reporting/move-discrepancies/` | get_move_discrepancy_list | can_access_reports |
| GET | `/reporting/move-discrepancies/download` | get_move_report_csv | can_access_reports |

## Request Types

**Prefix:** `/requests`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/requests/types` | get_request_type_list | can_access_request OR can_search (any_of) |
| GET | `/requests/types/{id}` | get_request_type_detail | can_access_request OR can_search (any_of) |
| POST | `/requests/types` | create_request_type | can_manage_groups_and_permissions |
| PATCH | `/requests/types/{id}` | update_request_type | can_manage_groups_and_permissions |
| DELETE | `/requests/types/{id}` | delete_request_type | can_manage_groups_and_permissions |

## Requests

**Prefix:** `/requests`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/requests/` | get_request_list | can_access_request |
| GET | `/requests/{id}` | get_request_detail | can_access_request |
| POST | `/requests/` | create_request | can_create_and_submit_manual_requests |
| PATCH | `/requests/{id}` | update_request | can_access_request |
| DELETE | `/requests/{id}` | delete_request | can_delete_request |

## Shelf Numbers

**Prefix:** `/shelves`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/shelves/numbers` | get_shelf_number_list | None (open) |
| GET | `/shelves/numbers/{id}` | get_shelf_number_detail | None (open) |
| POST | `/shelves/numbers` | create_shelf_number | can_manage_locations |
| PATCH | `/shelves/numbers/{id}` | update_shelf_number | can_manage_locations |
| DELETE | `/shelves/numbers/{id}` | delete_shelf_number | can_manage_locations |

## Shelf Position Numbers

**Prefix:** `/shelves/positions`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/shelves/positions/numbers` | get_shelf_position_number_list | None (open) |
| GET | `/shelves/positions/numbers/{id}` | get_shelf_position_number_detail | None (open) |
| POST | `/shelves/positions/numbers` | create_shelf_position_number | can_manage_locations |
| PATCH | `/shelves/positions/numbers/{id}` | update_shelf_position_number | can_manage_locations |
| DELETE | `/shelves/positions/numbers/{id}` | delete_shelf_position_number | can_manage_locations |

## Shelf Positions

**Prefix:** `/shelves/positions`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/shelves/positions/` | get_shelf_position_list | can_access_shelving OR can_move_trays_and_items_shelving_locations (any_of) |
| GET | `/shelves/positions/{id}` | get_shelf_position_detail | can_access_shelving OR can_move_trays_and_items_shelving_locations (any_of) |
| POST | `/shelves/positions/` | create_shelf_position | can_manage_locations |
| PATCH | `/shelves/positions/{id}` | update_shelf_position | can_manage_locations |
| DELETE | `/shelves/positions/{id}` | delete_shelf_position | can_manage_locations |

## Shelf Types

**Prefix:** `/shelf-types`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/shelf-types/` | get_shelf_type_list | can_manage_shelf_type |
| GET | `/shelf-types/{id}` | get_shelf_type_detail | can_manage_shelf_type |
| POST | `/shelf-types/` | create_shelf_type | can_manage_shelf_type |
| PATCH | `/shelf-types/{id}` | update_shelf_type | can_manage_shelf_type |
| DELETE | `/shelf-types/{id}` | delete_shelf_type | can_manage_shelf_type |

## Shelves

**Prefix:** `/shelves`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/shelves/` | get_shelf_list | can_manage_locations |
| GET | `/shelves/{id}` | get_shelf_detail | can_manage_locations |
| GET | `/shelves/barcode/{value}` | get_shelf_by_barcode_value | can_manage_locations |
| GET | `/shelves/barcode/{value}/shelved` | get_shelved_entities_by_shelf_barcode_value | can_manage_locations |
| POST | `/shelves/` | create_shelf | can_manage_locations |
| PATCH | `/shelves/{id}` | update_shelf | can_manage_locations |
| DELETE | `/shelves/{id}` | delete_shelf | can_manage_locations |

## Shelving Jobs

**Prefix:** `/shelving-jobs`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/shelving-jobs/` | get_shelving_job_list | can_access_shelving |
| GET | `/shelving-jobs/{id}` | get_shelving_job_detail | can_access_shelving |
| POST | `/shelving-jobs/` | create_shelving_job | can_create_and_execute_shelving_job |
| PATCH | `/shelving-jobs/{id}` | update_shelving_job | can_create_and_execute_shelving_job |
| DELETE | `/shelving-jobs/{id}` | delete_shelving_job | can_create_and_execute_shelving_job |
| POST | `/shelving-jobs/{id}/reassign-container-location` | reassign_container_location | can_move_trays_and_items OR can_create_and_execute_direct_shelving_job (any_of) |
| POST | `/shelving-jobs/{id}/reassign-proposed-location` | reassign_container_proposed_location | can_move_trays_and_items_shelving_locations |

## Side Orientations

**Prefix:** `/sides`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/sides/orientations` | get_side_orientation_list | None (open) |
| GET | `/sides/orientations/{id}` | get_side_orientation_detail | None (open) |
| POST | `/sides/orientations` | create_side_orientation | can_manage_locations |
| PATCH | `/sides/orientations/{id}` | update_side_orientation | can_manage_locations |
| DELETE | `/sides/orientations/{id}` | delete_side_orientation | can_manage_locations |

## Sides

**Prefix:** `/sides`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/sides/` | get_side_list | None (open) |
| GET | `/sides/{id}` | get_side_detail | None (open) |
| POST | `/sides/` | create_side | can_manage_locations |
| PATCH | `/sides/{id}` | update_side | can_manage_locations |
| DELETE | `/sides/{id}` | delete_side | can_manage_locations |

## Size Class

**Prefix:** `/size_class`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/size_class/` | get_size_class_list | can_manage_size_class |
| GET | `/size_class/{id}` | get_size_class_detail | can_manage_size_class |
| POST | `/size_class/` | create_size_class | can_manage_size_class |
| PATCH | `/size_class/{id}` | update_size_class | can_manage_size_class |
| DELETE | `/size_class/{id}` | delete_size_class | can_manage_size_class |

## Status

**Prefix:** `/status`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/status/` | get_api_status | None (open) |

## Subcollections

**Prefix:** `/subcollections`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/subcollections/` | get_subcollection_list | None (open) |
| GET | `/subcollections/{id}` | get_subcollection_detail | None (open) |
| POST | `/subcollections/` | create_subcollection | can_manage_locations |
| PATCH | `/subcollections/{id}` | update_subcollection | can_manage_locations |
| DELETE | `/subcollections/{id}` | delete_subcollection | can_manage_locations |

## Trays

**Prefix:** `/trays`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/trays/` | get_tray_list | can_access_tray_detail |
| GET | `/trays/{id}` | get_tray_detail | can_access_tray_detail |
| GET | `/trays/barcode/{value}` | get_tray_by_barcode_value | can_access_tray_detail |
| POST | `/trays/` | create_tray | can_access_accession |
| PATCH | `/trays/{id}` | update_tray | can_access_accession OR can_access_verification (any_of) |
| DELETE | `/trays/{id}` | delete_tray | can_access_accession |
| POST | `/trays/move/{barcode_value}` | move_tray | can_move_trays_and_items |

## Users

**Prefix:** `/users`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/users/` | get_user_list | None (open) |
| GET | `/users/{id}` | get_user_detail | None (open) |
| GET | `/users/{id}/groups` | get_user_groups | None (open) |
| GET | `/users/{user_id}/permissions` | get_user_permissions | None (open) |
| POST | `/users/` | create_user | can_manage_users |
| PATCH | `/users/{id}` | update_user | can_manage_users |
| DELETE | `/users/{id}` | delete_user | can_manage_users |

## Verification Changes

**Prefix:** `/verification-changes`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/verification-changes/` | get_verification_change_list | can_access_verification |
| GET | `/verification-changes/{id}` | get_verification_change_detail | can_access_verification |
| POST | `/verification-changes/` | create_verification_change | can_access_verification |
| PATCH | `/verification-changes/{id}` | update_verification_change | can_access_verification |
| DELETE | `/verification-changes/{id}` | delete_verification_change | can_access_verification |

## Verification Jobs

**Prefix:** `/verification-jobs`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/verification-jobs/` | get_verification_job_list | can_access_verification |
| GET | `/verification-jobs/dropdown/` | get_verification_job_list_lite | can_access_verification |
| GET | `/verification-jobs/{id}` | get_verification_job_detail | can_access_verification |
| GET | `/verification-jobs/by-accession-job-id/{id}` | get_verification_job_id_by_acc_job_id | can_access_verification |
| GET | `/verification-jobs/workflow/{id}` | get_verification_job_detail_by_workflow | can_access_verification |
| POST | `/verification-jobs/` | create_verification_job | can_access_verification |
| PATCH | `/verification-jobs/{id}` | update_verification_job | can_access_verification |
| DELETE | `/verification-jobs/{id}` | delete_verification_job | can_cancel_verification_job |
| PATCH | `/verification-jobs/{id}/add` | add_item_to_verification_job | can_access_verification |
| PATCH | `/verification-jobs/{id}/remove` | remove_item_from_verification_job | can_access_verification |

## Withdraw Jobs

**Prefix:** `/withdraw-jobs`

| Method | Path | Function | Permission(s) |
|--------|------|----------|---------------|
| GET | `/withdraw-jobs/` | get_withdraw_job_list | can_access_withdraw |
| GET | `/withdraw-jobs/{id}` | get_withdraw_job_detail | can_access_withdraw |
| POST | `/withdraw-jobs/` | create_withdraw_job | can_access_withdraw |
| PATCH | `/withdraw-jobs/{id}` | update_withdraw_job | can_access_withdraw |
| DELETE | `/withdraw-jobs/{job_id}` | delete_withdraw_job | can_access_withdraw |
| POST | `/withdraw-jobs/{job_id}/add_items` | add_items_to_withdraw_job | can_access_withdraw |
| DELETE | `/withdraw-jobs/{job_id}/remove_items` | remove_items_from_withdraw_job | can_access_withdraw |
