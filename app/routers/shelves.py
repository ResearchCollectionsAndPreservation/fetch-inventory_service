import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from sqlmodel import Session, select
from sqlalchemy import text
from datetime import datetime, timezone
from fastapi_pagination import Page
from fastapi_pagination import paginate as paginate_list
from fastapi_pagination.ext.sqlmodel import paginate

from app.database.session import get_session
from app.permissions import require_permissions
from app.filter_params import SortParams
from app.models.owners import Owner
from app.models.shelf_types import ShelfType
from app.models.shelves import Shelf
from app.models.barcodes import Barcode
from app.models.shelf_numbers import ShelfNumber
from app.models.buildings import Building
from app.models.modules import Module
from app.models.aisles import Aisle
from app.models.sides import Side
from app.models.ladders import Ladder
from app.models.size_class import SizeClass
from app.models.trays import Tray
from app.models.non_tray_items import NonTrayItem
from app.models.shelf_positions import ShelfPosition
from app.filter_params import ShelfFilterParams
from app.models.shelf_position_numbers import ShelfPositionNumber
from app.schemas.shelves import (
    ShelfInput,
    ShelfUpdateInput,
    ShelfListOutput,
    ShelfDetailWriteOutput,
    ShelfDetailReadOutput,
)
from app.config.exceptions import NotFound, ValidationException, InternalServerError
from app.sorting import ShelvingSorter
from app.utilities import get_sorted_query, start_session_with_audit_info

router = APIRouter(
    prefix="/shelves",
    tags=["shelves"],
)

LOGGER = logging.getLogger("app.routers.shelves")


@router.get("/", response_model=Page[ShelfListOutput])
def get_shelf_list(
    session: Session = Depends(get_session),
    params: ShelfFilterParams = Depends(),
    sort_params: SortParams = Depends(),
    search: Optional[str] = Query(None, description="Search by Shelf location"),
    _: bool = Depends(require_permissions("can_manage_locations"))
) -> list:
    """
    Get a list of shelves.

    **Parameters:**
    - params: The filter parameters.
        - building_id (int): The ID of the building to filter by.
        - module_id (int): The ID of the module to filter by.
        - aisle_id (int): The ID of the aisle to filter by.
        - side_id (int): The ID of the side to filter by.
        - ladder_id (int): The ID of the ladder to filter by.
        - shelf_id (int): The ID of the shelf to filter by.
        - owner_id (int): The ID of the owner to filter by.
        - size_class_id (int): The ID of the size class to filter by.
        - unassigned (bool): Whether to include unassigned shelves.
        - location (str): Lookup against external location
    - sort_params (SortParams): The sorting parameters.
        - sort_by (str): The field to sort by.
        - sort_order (str): The order to sort by.
    - search (Optional[str]): The search query.
        - location: The location to search for.

    **Returns:**
    - Shelf List Output: The paginated list of shelves.
    """
    shelf_queryset = select(Shelf)

    if search:
        shelf_queryset = shelf_queryset.where(Shelf.location.icontains(search))

    if params.owner_id:
        shelf_queryset = shelf_queryset.where(Shelf.owner_id == params.owner_id)

    if params.size_class_id:
        shelf_queryset = shelf_queryset.join(
            ShelfType, Shelf.shelf_type_id == ShelfType.id
        ).where(ShelfType.size_class_id == params.size_class_id)

    # location from most to least constrained
    if params.shelf_id:
        shelf_queryset = shelf_queryset.where(Shelf.id == params.shelf_id)
    elif params.ladder_id:
        shelf_queryset = shelf_queryset.join(
            Ladder, Shelf.ladder_id == Ladder.id
        ).where(Ladder.id == params.ladder_id)
    elif params.side_id:
        shelf_queryset = (
            shelf_queryset.join(Ladder, Shelf.ladder_id == Ladder.id)
            .join(Side, Ladder.side_id == Side.id)
            .where(Side.id == params.side_id)
        )
    elif params.aisle_id:
        shelf_queryset = (
            shelf_queryset.join(Ladder, Shelf.ladder_id == Ladder.id)
            .join(Side, Ladder.side_id == Side.id)
            .join(Aisle, Side.aisle_id == Aisle.id)
            .where(Aisle.id == params.aisle_id)
        )
    elif params.module_id:
        shelf_queryset = (
            shelf_queryset.join(Ladder, Shelf.ladder_id == Ladder.id)
            .join(Side, Ladder.side_id == Side.id)
            .join(Aisle, Side.aisle_id == Aisle.id)
            .join(Module, Aisle.module_id == Module.id)
            .where(Module.id == params.module_id)
        )
    elif params.building_id:
        shelf_queryset = (
            shelf_queryset.join(Ladder, Shelf.ladder_id == Ladder.id)
            .join(Side, Ladder.side_id == Side.id)
            .join(Aisle, Side.aisle_id == Aisle.id)
            .join(Module, Aisle.module_id == Module.id)
            .join(Building, Module.building_id == Building.id)
            .where(Building.id == params.building_id)
        )

    if params.unassigned:
        shelf_queryset = shelf_queryset.where(Shelf.barcode_id == None)
    if params.barcode_value:
        barcode_value_subquery = select(Barcode.id).where(
            Barcode.value == params.barcode_value
        )
        shelf_queryset = shelf_queryset.where(
            Shelf.barcode_id == barcode_value_subquery
        )
    if params.owner:
        owner_subquery = select(Owner.id).where(Owner.name == params.owner)
        shelf_queryset = shelf_queryset.where(Shelf.owner_id == owner_subquery)
    if params.size_class:
        size_class_subquery = select(SizeClass.id).where(
            SizeClass.name == params.size_class
        )
        shelf_queryset = shelf_queryset.join(
            ShelfType, Shelf.shelf_type_id == ShelfType.id
        ).where(ShelfType.size_class_id == size_class_subquery)
    if params.location:
        shelf_queryset = shelf_queryset.where(Shelf.location == params.location)

    # Validate and Apply sorting based on sort_params
    if sort_params.sort_by:
        sorter = ShelvingSorter(Shelf)
        shelf_queryset = sorter.apply_sorting(shelf_queryset, sort_params)

    return paginate(session, shelf_queryset)


@router.get("/{id}", response_model=ShelfDetailReadOutput)
def get_shelf_detail(id: int, session: Session = Depends(get_session), _: bool = Depends(require_permissions("can_manage_locations"))):
    """
    Retrieves the details of a shelf with the given ID.

    **Args:**
    - id: The ID of the shelf to retrieve.

    **Returns:**
    - Shelf Detail Read Output: The details of the retrieved shelf.

    **Raises:**
    - HTTPException: If the shelf with the specified ID is not found.
    """
    shelf = session.get(Shelf, id)

    if shelf:
        return shelf

    raise NotFound(detail=f"Shelf ID {id} Not Found")


@router.get("/barcode/{value}", response_model=ShelfDetailReadOutput)
def get_shelf_by_barcode_value(value: str, session: Session = Depends(get_session), _: bool = Depends(require_permissions("can_manage_locations"))):
    """
    Retrieve a shelf using a barcode value

    **Parameters:**
    - value (str): The value of the barcode to retrieve.
    """
    statement = select(Shelf).join(Barcode).where(Barcode.value == value)
    shelf = session.exec(statement).first()
    if not shelf:
        raise NotFound(detail=f"Shelf with barcode value {value} not found")
    return shelf


@router.get("/barcode/{value}/shelved", response_model=Page[dict])
def get_shelved_entities_by_shelf_barcode_value(
    value: str, session: Session = Depends(get_session), _: bool = Depends(require_permissions("can_manage_locations"))
):
    """
    Retrieve tray and non_tray barcode list from things on a shelf
    using a shelf barcode value

    **Parameters:**
    - value (str): The value of the barcode to retrieve.
    """
    shelf_statement = select(Shelf).join(Barcode).where(Barcode.value == value)
    shelf = session.exec(shelf_statement).first()
    if not shelf:
        raise NotFound(detail=f"Shelf with barcode value {value} not found")

    shelf_positions = list(
        session.exec(
            select(ShelfPosition).where(ShelfPosition.shelf_id == shelf.id)
        ).all()
    )

    trays = {
        t.shelf_position_id: t
        for t in session.exec(
            select(Tray)
            .join(Barcode, Tray.barcode_id == Barcode.id)
            .where(Tray.shelf_position_id.in_([p.id for p in shelf_positions]))
        ).all()
    }

    non_trays = {
        nt.shelf_position_id: nt
        for nt in session.exec(
            select(NonTrayItem)
            .join(Barcode, NonTrayItem.barcode_id == Barcode.id)
            .where(NonTrayItem.shelf_position_id.in_([p.id for p in shelf_positions]))
        ).all()
    }

    results = []
    for shelf_position in shelf_positions:
        # Get the position number from the related shelf_position_number object.
        position_number = shelf_position.shelf_position_number.number

        if shelf_position.id in trays:
            results.append(
                {
                    "type": "tray",
                    "barcode_value": trays[shelf_position.id].barcode.value,
                    "shelf_position_number": position_number, # <-- ADD THIS LINE
                }
            )
        elif shelf_position.id in non_trays:
            results.append(
                {
                    "type": "non_tray",
                    "barcode_value": non_trays[shelf_position.id].barcode.value,
                    "shelf_position_number": position_number, # <-- AND THIS LINE
                }
            )

    return paginate_list(results)


# Replace your existing create_shelf function with this one

@router.post("/", response_model=ShelfDetailWriteOutput, status_code=201)
def create_shelf(
    shelf_input: ShelfInput, session: Session = Depends(get_session), _: bool = Depends(require_permissions("can_manage_locations"))
) -> Shelf:
    """
    Create a shelf and efficiently bulk-creates its associated shelf positions.

    **Args:**
    - Shelf Input: The input data for creating the shelf.

    **Returns:**
    - Shelf Detail Write Output: The newly created shelf.
    """
    try:
        # --- (Initial setup code is the same) ---
        shelf_number = shelf_input.shelf_number
        shelf_number_id = shelf_input.shelf_number_id
        mutated_data = shelf_input.model_dump(exclude="shelf_number")
        audit_info = getattr(session, "audit_info", {"name": "System", "id": "0"})

        if not shelf_number_id and not shelf_number:
            raise ValidationException(
                detail="shelf_number_id OR shelf_number required"
            )
        elif shelf_number and not shelf_number_id:
            shelf_num_object = session.exec(select(ShelfNumber).where(ShelfNumber.number == shelf_number)).first()
            if not shelf_num_object:
                raise ValidationException(
                    detail=f"No shelf_number entity exists for shelf number {shelf_number}"
                )
            mutated_data["shelf_number_id"] = shelf_num_object.id

        new_shelf = Shelf(**mutated_data)
        session.add(new_shelf)
        session.commit()
        session.refresh(new_shelf)

        shelf_type = session.get(ShelfType, new_shelf.shelf_type_id)
        if not shelf_type:
             raise InternalServerError(detail=f"ShelfType ID {new_shelf.shelf_type_id} not found.")

        # --- START: OPTIMIZED SHELF POSITION CREATION ---

        # 1. Fetch all required ShelfPositionNumber objects in a single query.
        required_numbers = list(range(1, shelf_type.max_capacity + 1))
        position_numbers_query = select(ShelfPositionNumber).where(ShelfPositionNumber.number.in_(required_numbers))
        
        # 2. Create a dictionary map for instant O(1) lookups. This is much faster than list searching.
        position_numbers_map = {p.number: p for p in session.exec(position_numbers_query).all()}

        shelf_position_list = []
        for position_num in required_numbers:
            # 3. Get the object from the local map instead of querying the database in a loop.
            shelf_pos_num_obj = position_numbers_map.get(position_num)
            
            if not shelf_pos_num_obj:
                raise InternalServerError(detail=f"ShelfPositionNumber for position {position_num} not found in database.")

            shelf_position_list.append({
                "shelf_id": new_shelf.id,
                "shelf_position_number_id": shelf_pos_num_obj.id,
            })

        # --- END: OPTIMIZED SHELF POSITION CREATION ---

        if shelf_position_list:
            shelf_positions_to_create: List[ShelfPosition] = [
                ShelfPosition(**data) for data in shelf_position_list
            ]
            session.add_all(shelf_positions_to_create)
            start_session_with_audit_info(audit_info, session)
            session.commit()
        
        # Re-calculate available space.
        if hasattr(new_shelf, 'calc_available_space'):
            new_shelf.calc_available_space(session=session)
            session.add(new_shelf)
            session.commit()
            session.refresh(new_shelf)

        return new_shelf
    except Exception as e:
        raise InternalServerError(detail=f"{e}")


@router.patch("/{id}", response_model=ShelfDetailWriteOutput)
def update_shelf(
    id: int, shelf_input: ShelfUpdateInput, session: Session = Depends(get_session), _: bool = Depends(require_permissions("can_manage_locations"))
):
    """
    Update a shelf with the given ID.
    If the shelf_type is changed, this will adjust the number of shelf positions
    to match the new max_capacity, but only if the positions being removed are empty.

    **Args:**
    - id: The ID of the shelf to update.
    - Shelf Update Input: The updated shelf data.

    **Raises:**
    - HTTPException: If the shelf with the given ID does not exist.
    - ValidationException: If attempting to downsize a shelf with occupied positions.

    **Returns:**
    - Shelf Detail Write Output: The updated shelf.
    """
    existing_shelf = session.get(Shelf, id)

    if existing_shelf is None:
        raise NotFound(detail=f"Shelf ID {id} Not Found")

    mutated_data = shelf_input.model_dump(exclude_unset=True)

    # --- START: LOGIC TO HANDLE SHELF CAPACITY CHANGES ---
    
    # Check if the shelf_type_id is being changed and is different from the current one.
    if "shelf_type_id" in mutated_data and mutated_data["shelf_type_id"] != existing_shelf.shelf_type_id:
        
        # Get the old and new shelf type objects to compare their capacities.
        old_shelf_type = session.get(ShelfType, existing_shelf.shelf_type_id)
        new_shelf_type = session.get(ShelfType, mutated_data["shelf_type_id"])

        if not new_shelf_type:
            raise ValidationException(detail=f"New Shelf Type ID {mutated_data['shelf_type_id']} not found.")

        old_capacity = old_shelf_type.max_capacity if old_shelf_type else 0
        new_capacity = new_shelf_type.max_capacity

        # --- PATH 1: DECREASING CAPACITY ---
        if new_capacity < old_capacity:
            # We need to remove positions, but first, we must verify they are empty.
            num_to_remove = old_capacity - new_capacity

            # Find the positions with the highest numbers to remove them.
            # We need to join to sort by the actual position number.
            positions_to_check_query = (
                select(ShelfPosition)
                .join(ShelfPositionNumber, ShelfPosition.shelf_position_number_id == ShelfPositionNumber.id)
                .where(ShelfPosition.shelf_id == id)
                .order_by(ShelfPositionNumber.number.desc())
                .limit(num_to_remove)
            )
            positions_to_delete = session.exec(positions_to_check_query).all()
            position_ids_to_delete = [p.id for p in positions_to_delete]

            if position_ids_to_delete:
                # Check for ANY trays or non-tray items in the positions slated for deletion.
                # This is an efficient check that stops at the first occupied slot it finds.
                occupied_tray = session.exec(select(Tray.id).where(Tray.shelf_position_id.in_(position_ids_to_delete)).limit(1)).first()
                occupied_non_tray = session.exec(select(NonTrayItem.id).where(NonTrayItem.shelf_position_id.in_(position_ids_to_delete)).limit(1)).first()

                if occupied_tray or occupied_non_tray:
                    # An item exists! Block the update and throw a clear error.
                    raise ValidationException(detail="Shelf is not Empty")

                # If we reach here, the positions are empty and safe to delete.
                for pos in positions_to_delete:
                    session.delete(pos)
        
        # --- PATH 2: INCREASING CAPACITY ---
        elif new_capacity > old_capacity:
            # We need to add new empty positions.
            # Fetch all required ShelfPositionNumber objects in one query to avoid N+1.
            new_position_numbers_range = list(range(old_capacity + 1, new_capacity + 1))
            
            position_numbers_query = (
                session.query(ShelfPositionNumber)
                .filter(ShelfPositionNumber.number.in_(new_position_numbers_range))
            )
            position_numbers_map = {p.number: p for p in position_numbers_query.all()}

            for position_num in new_position_numbers_range:
                shelf_pos_num_obj = position_numbers_map.get(position_num)
                if not shelf_pos_num_obj:
                    raise InternalServerError(detail=f"ShelfPositionNumber for position {position_num} not found in database.")

                new_position = ShelfPosition(
                    shelf_id=id,
                    shelf_position_number_id=shelf_pos_num_obj.id,
                )
                session.add(new_position)

    # --- END: LOGIC TO HANDLE SHELF CAPACITY CHANGES ---

    # Apply all other attribute changes from the request.
    for key, value in mutated_data.items():
        setattr(existing_shelf, key, value)

    # Update the timestamp and commit all changes (deletes, adds, updates).
    setattr(existing_shelf, "update_dt", datetime.now(timezone.utc))
    session.add(existing_shelf)
    session.commit()
    session.refresh(existing_shelf)

    # Re-calculate available space now that positions have changed.
    # This might be a custom method on your Shelf model.
    if hasattr(existing_shelf, 'calc_available_space'):
        existing_shelf.calc_available_space(session=session)
        session.add(existing_shelf)
        session.commit()
        session.refresh(existing_shelf)

    return existing_shelf


@router.delete("/{id}")
def delete_shelf(id: int, session: Session = Depends(get_session), _: bool = Depends(require_permissions("can_manage_locations"))):
    """
    Delete a shelf by its ID.

    **Args:**
    - id: The ID of the shelf to delete.

    **Raises:**
    - HTTPException: If the shelf with the given id is not found.

    **Returns:**
    - None
    """
    shelf = session.get(Shelf, id)

    if shelf:
        session.delete(shelf)
        session.commit()

        return HTTPException(
            status_code=204, detail=f"Shelf ID {id} Deleted Successfully"
        )

    raise NotFound(detail=f"Shelf ID {id} Not Found")
