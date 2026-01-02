from core.models.db.manufacturer import Address


def dedupe_addresses(addresses: list[Address]):
    i = 0
    while i < len(addresses):
        j = i + 1
        if j >= len(addresses):
            break

        Address_A = addresses[i]
        Address_B = addresses[j]
        merged_AB = merge_addresses_A_and_B(A=Address_A, B=Address_B)
        if merged_AB:
            addresses[i] = merged_AB
            addresses.pop(j)
        else:
            i += 1


def can_addresses_A_and_B_merge(A: Address, B: Address) -> bool:
    if A.base_hash() != B.base_hash():
        return False
    # elif A.latitude and B.latitude and (A.latitude != B.latitude):
    #     return False
    # elif A.longitude and B.longitude and (A.longitude != B.longitude):
    #     return False
    elif A.postal_code and B.postal_code and (A.postal_code != B.postal_code):
        return False

    if not A.address_lines or not B.address_lines:
        return True

    A_addr_line_set = {addr_line for addr_line in A.address_lines}
    B_addr_line_set = {addr_line for addr_line in B.address_lines}

    return bool(A_addr_line_set | B_addr_line_set)  # atleast one address line matches


def merge_addresses_A_and_B(A: Address, B: Address) -> Address | None:
    if not can_addresses_A_and_B_merge(A, B):
        return None

    A_phone_nums = (
        {phone_num for phone_num in A.phone_numbers} if A.phone_numbers else set()
    )
    B_phone_nums = (
        {phone_num for phone_num in B.phone_numbers} if B.phone_numbers else set()
    )

    A_fax_nums = {fax_num for fax_num in A.fax_numbers} if A.fax_numbers else set()
    B_fax_nums = {fax_num for fax_num in B.fax_numbers} if B.fax_numbers else set()

    return Address(
        city=A.city or B.city,
        state=A.state or B.state,
        country=A.country or B.country,
        address_lines=A.address_lines or B.address_lines,
        name=A.name or B.name,
        county=A.county or B.county,
        postal_code=A.postal_code or B.postal_code,
        latitude=A.latitude,
        longitude=A.longitude,
        phone_numbers=list(A_phone_nums & B_phone_nums),
        fax_numbers=list(A_fax_nums & B_fax_nums),
    )
