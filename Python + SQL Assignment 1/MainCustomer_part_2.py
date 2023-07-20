import click
import logging
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, func
from sqlalchemy.orm import declarative_base, sessionmaker

# Create the engine and session
engine = create_engine('postgresql://postgres:j4d3r@localhost/postgres')
Session = sessionmaker(bind=engine)
session = Session()

# Create a base class for declarative models
Base = declarative_base()

# Define the Customer model
class Customer(Base):
    __tablename__ = 'customer'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    address = Column(String(512))
    dob = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.group()
def cli():
    pass

@cli.command()
@click.option('--first-name', prompt=True, help='First name of the customer')
@click.option('--last-name', prompt=True, help='Last name of the customer')
@click.option('--address', prompt=True, help='Address of the customer')
@click.option('--dob', prompt=True, help='Date of birth of the customer (YYYY-MM-DD HH:MM:SS)')
def create(first_name, last_name, address, dob):
    try:
        customer = Customer(first_name=first_name, last_name=last_name, address=address,
                            dob=dob, updated_at=func.now())
        session.add(customer)
        session.commit()
        logger.info(f"Customer {customer.id} created successfully.")
    except Exception as e:
        session.rollback()
        logger.exception(f"Error creating customer: {e}")

@cli.command()
@click.argument('customer_id', type=int)
@click.option('--first-name', prompt=True, help='First name of the customer')
@click.option('--last-name', prompt=True, help='Last name of the customer')
@click.option('--address', prompt=True, help='Address of the customer')
@click.option('--dob', prompt=True, help='Date of birth of the customer (YYYY-MM-DD HH:MM:SS)')
def update(customer_id, first_name, last_name, address, dob):
    try:
        customer = session.get(Customer, customer_id)
        if customer:
            customer.first_name = first_name
            customer.last_name = last_name
            customer.address = address
            customer.dob = dob
            customer.updated_at = func.now()
            session.commit()
            logger.info(f"Customer {customer_id} updated successfully.")
        else:
            logger.warning(f"Customer with ID {customer_id} not found.")
    except Exception as e:
        session.rollback()
        logger.exception(f"Error updating customer: {e}")


@cli.command()
@click.argument('customer_id', type=int)
def delete(customer_id):
    try:
        customer = session.get(Customer, customer_id)
        if customer:
            session.delete(customer)
            session.commit()
            logger.info(f"Customer {customer_id} deleted successfully.")
        else:
            logger.warning(f"Customer with ID {customer_id} not found.")
    except Exception as e:
        session.rollback()
        logger.exception(f"Error deleting customer: {e}")

@cli.command()
def list():
    try:
        customers = session.query(Customer).all()
        for customer in customers:
            print(f"Customer ID: {customer.id}")
            print(f"Name: {customer.first_name} {customer.last_name}")
            print(f"Address: {customer.address}")
            print(f"Date of Birth: {customer.dob}")
            print(f"Updated At: {customer.updated_at}")
            print("")
    except Exception as e:
        logger.exception(f"Error listing customers: {e}")
@cli.command()
@click.argument('customer_id', type=int)
def find(customer_id):
    try:
        customer = session.get(Customer, customer_id)
        if customer:
            print(f"Customer ID: {customer.id}")
            print(f"Name: {customer.first_name} {customer.last_name}")
            print(f"Address: {customer.address}")
            print(f"Date of Birth: {customer.dob}")
            print(f"Updated At: {customer.updated_at}")
            print("")
        else:
            logger.warning(f"Customer with ID {customer_id} not found.")
    except Exception as e:
        logger.exception(f"Error searching customer: {e}")

if __name__ == '__main__':
    cli()
