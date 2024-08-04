# Room Price Genie Test 01
## ARCHITECTURE
### 1. Data Provider Service

- Handles incoming booking and cancellation events.
- Uses RabbitMQ to publish events, and subscriber to save to MongoDB.

### 2. Dashboard Service

- Periodically polls Data Provider from GET /events.
- Processes events and updates its internal database.
- Exposes the GET /dashboard endpoint to provide aggregated data.

### 3. RabbitMQ

- Acts as a broker to facilitate communication between the Data Provider and Dashboard Service.
- Ensures reliable message delivery and decoupling of services specifically between Data Provider and Simulator of hotel orders as a robust solution for the hotel booking system.

### 4. MongoDB
- Stores persistent data for both services.
- Uses indexing for efficient querying.

## DESIGN PATTERNS
Event-driven architecture is used as it allows services to communicate asynchronously via events. This pattern decouples the services and enables scalability and resilience.

Workflow:
1. Event Producers:
   - Data Provider Service: Acts as an event producer by publishing booking and cancellation events to RabbitMQ.

2. Event Consumers:
   - Dashboard Service: Acts as an event consumer by polling events from the Data Provider and processing them to update its internal database.

3. Event Broker:
   - RabbitMQ: Serves as the event broker, facilitating communication between the Data Provider and the Simulator of hotel orders. It ensures reliable message delivery and decouples the services.

4. Event Storage:
   - MongoDB: Acts as the persistent storage for events. Both services can use MongoDB to store and query events efficiently, leveraging indexing.

## DASHBOARD REQUEST EXPLANATION
Dashboard Service will request data from Data Provider Service via GET /events to get number of booking (per night of stay) as of bellow:
```bash
params = {
    "night_of_stay__gte": start_date.strftime("%Y-%m-%d"),
    "night_of_stay__lte": end_date.strftime("%Y-%m-%d"),
    "rpg_status": 1
}
```
it will gather data per year, from 5 years ago to current year. Data found will be aggregated per year.

## PREPARATION
rename .env.example (in root directory) to .env to set the environment variables

## RUN SERVICES WITHIN DOCKER
```bash
docker-compose up -d --build
```

## TEST DATA PROVIDER WITHIN DOCKER
```bash
docker-compose run data-provider-tests
```

## RUN SERVICES MANUALLY
### Run Service Data Provider
```bash
# cd Data-Provider-Service
uvicorn dataprovider:app --reload --port 8000
```

### Run Service Dashboard
```bash
# cd Dashboard-Service
uvicorn dashboard:app --reload --port 7777
```

### Simulation
```bash
python Simulator/hotel_order_simulator.py
```

### Run Test Data Provider
```bash
# cd Data-Provider-Service (Make sure you are in the right directory)
pytest tests/test_dprovider.py
```

## DOCUMENTATION
### Data Provider Service
```bash
http://localhost:8000/documentation
```

### Dashboard Service
```bash
http://localhost:7777/documentation
```  