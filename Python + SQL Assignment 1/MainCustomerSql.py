from sqlalchemy import create_engine, text
from sqlalchemy.sql import func

# Create the engine
engine = create_engine("postgresql://postgres:password@localhost/postgres")

# Create a customer
def create_customer(first_name, last_name, address, dob, updated_at):
    query = f"""
        INSERT INTO customer (first_name, last_name, address, dob, updated_at)
        VALUES ('{first_name}', '{last_name}', '{address}', '{dob}', '{updated_at}')
        RETURNING id
    """
    with engine.connect() as connection:
        result = connection.execute(text(query))
        customer_id = result.scalar()
        connection.commit()
    return customer_id

# Retrieve a customer by ID
def get_customer_by_id(customer_id):
    query = f"SELECT * FROM customer WHERE id = {customer_id}"
    with engine.connect() as connection:
        result = connection.execute(text(query))
        customer = result.fetchone()
    return customer

# Update a customer's information
def update_customer(customer_id, first_name, last_name, address, dob, updated_at):
    query = f"""
        UPDATE customer SET first_name = '{first_name}', last_name = '{last_name}',
        address = '{address}', dob = '{dob}', updated_at = '{updated_at}'
        WHERE id = {customer_id}
    """
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit()

# Delete a customer
def delete_customer(customer_id):
    query = f"DELETE FROM customer WHERE id = {customer_id}"
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit()

# Example usage
customer_id = create_customer("tiago", "belarmino", "Brasilia, Brazil", '2009-12-06', func.now())
print("Created customer:", customer_id)

customer = get_customer_by_id(customer_id)
print("Retrieved customer:", customer)

update_customer(customer_id, "Jane", "Smith", "456 Elm St", '2009-12-06', func.now())
print("Updated customer")

delete_customer(customer_id)
print("Deleted customer")
