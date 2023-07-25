import csv
import json
import logging
import click
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, BigInteger, JSON, and_
from sqlalchemy.orm import declarative_base, sessionmaker

# Create the engine and session
engine = create_engine('postgresql://postgres:password@localhost/postgres')
Session = sessionmaker(bind=engine)
session = Session()

# Create a base class for declarative models
Base = declarative_base()


# Define the Customer model
class Customer(Base):
    __tablename__ = 'customer_partner_relation_table'
    customer_status = Column(String(255), default='MIGRATION_COMPLETE')
    partner_status = Column(String(255), default='MIGRATION_COMPLETE')
    cpqmodel = Column(String(255), primary_key=True)
    customer_account_type = Column(String(255))
    customer_sfdc_account_id = Column(String(255), primary_key=True)
    customer_woc_ref = Column(BigInteger)
    package = Column(String(255), primary_key=True)
    partner_sfdc_account_id = Column(String(255), primary_key=True)
    total_qty = Column(Integer)
    subscription_lines = Column(JSON)
    entities = Column(JSON)


# Define the Subscription model
class Subscription(Base):
    __tablename__ = 'subscription_table'
    id = Column(Integer, primary_key=True)
    qty = Column(Integer)
    cpqmodel = Column(String(255))
    package = Column(String(255))
    customer_sfdc_account_id = Column(String(255))
    allocated_qty = Column(Integer)
    subscription_line_id = Column(BigInteger)


# Create the tables if they don't exist
Base.metadata.create_all(engine)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_subscription_lines(subscription_lines_data):
    try:
        updated_lines_data = []
        for line in subscription_lines_data:
            line['allocated_qty'] = 0
            updated_lines_data.append(line)
        return json.dumps(updated_lines_data)
    except Exception as e:
        logger.exception(f"Error processing subscription lines: {e}")
        return None


def process_customer(customer_sfdc_account_id):
    try:
        # Load CSV data into customer_partner_relation_table for the specified customer_sfdc_account_id
        with open('one_batch.csv', newline='') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if row['customer_sfdc_account_id'] == customer_sfdc_account_id:
                    customer = Customer(**row)
                    customer.subscription_lines = row['subscription_lines']
                    customer.customer_status = 'IN_PROGRESS'
                    session.add(customer)
                    session.commit()

                    # Process the customer's data
                    subscription_lines_data = json.loads(customer.subscription_lines)
                    if subscription_lines_data is None:
                        return

                    for line in subscription_lines_data:
                        subscription_data = {
                            'qty': line['qty'],
                            'cpqmodel': customer.cpqmodel,
                            'package': customer.package,
                            'customer_sfdc_account_id': customer.customer_sfdc_account_id,
                            'allocated_qty': line['allocated_qty'],
                            'subscription_line_id': line['subscription_line_id']
                        }
                        subscription = Subscription(**subscription_data)
                        session.add(subscription)
                        session.commit()
                    customer.subscription_lines = process_subscription_lines(subscription_lines_data);
                    customer.customer_status = 'READY'
                    session.commit()
        logger.info(f"Data processing for customer_sfdc_account_id {customer_sfdc_account_id} completed successfully.")
    except Exception as e:
        session.rollback()
        logger.exception(f"Error processing data for customer_sfdc_account_id {customer_sfdc_account_id}: {e}")


def search_customers(customer_sfdc_account_id, partner_sfdc_account_id, cpqmodel, package, customer_status=None,
                     partner_status=None):
    try:
        # Define the filter conditions based on the provided criteria
        filters = []
        if customer_sfdc_account_id:
            filters.append(Customer.customer_sfdc_account_id == customer_sfdc_account_id)
        if partner_sfdc_account_id:
            filters.append(Customer.partner_sfdc_account_id == partner_sfdc_account_id)
        if cpqmodel:
            filters.append(Customer.cpqmodel == cpqmodel)
        if package:
            filters.append(Customer.package == package)
        if customer_status is not None:
            filters.append(Customer.customer_status == customer_status)
        if partner_status is not None:
            filters.append(Customer.partner_status == partner_status)

        # Query the database with the filter conditions
        return session.query(Customer).filter(and_(*filters)).all()
    except Exception as e:
        print(f"Error searching customers: {e}")


def process_partner(partner_sfdc_account_id):
    try:
        # Load CSV data into customer_partner_relation_table for all customers associated with the partner_sfdc_account_id
        with open('one_batch.csv', newline='') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if row['partner_sfdc_account_id'] == partner_sfdc_account_id:
                    if search_customers(customer_sfdc_account_id=row['customer_sfdc_account_id'],
                                        partner_sfdc_account_id=row['partner_sfdc_account_id'],
                                        cpqmodel=row['cpqmodel'], package=row['package'], customer_status='READY'):
                        continue
                    else:
                        # Process all customers associated with the partner_sfdc_account_id
                        process_customer(row['customer_sfdc_account_id'])

        # Update partner status
        partner_customers = session.query(Customer).filter_by(partner_sfdc_account_id=partner_sfdc_account_id).all()
        if all(customer.customer_status == 'READY' for customer in partner_customers):
            for partner_customer in partner_customers:
                partner_customer.partner_status = 'READY'
            session.commit()

        logger.info(f"Data processing for partner_sfdc_account_id {partner_sfdc_account_id} completed successfully.")
    except Exception as e:
        session.rollback()
        logger.exception(f"Error processing data for partner_sfdc_account_id {partner_sfdc_account_id}: {e}")


@click.command()
@click.option('--customer_id', help='Process data for the specified customer.')
@click.option('--partner_id', help='Process data for all customers associated with the specified partner.')
def process_data(customer_id, partner_id):
    if customer_id:
        process_customer(customer_id)
    elif partner_id:
        process_partner(partner_id)
    else:
        # Load CSV data into customer_partner_relation_table for all customers
        with open('one_batch.csv', newline='') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if search_customers(customer_sfdc_account_id=row['customer_sfdc_account_id'],
                                    partner_sfdc_account_id=row['partner_sfdc_account_id'],
                                    cpqmodel=row['cpqmodel'], package=row['package'], partner_status='READY'):
                    continue
                else:
                    process_partner(row['partner_sfdc_account_id'])


if __name__ == '__main__':
    process_data()
