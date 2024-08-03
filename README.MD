
RUN TEST DATA PROVIDER
pytest tests/test_dprovider.py

RUN SERVICE DATA PROVIDER
uvicorn dataprovider:app --reload --port 8000

RUN SERVICE DASHBOARD
uvicorn dashboard:app --reload --port 7000

RUN HOTEL ORDER SIMULATION 
python Simulator/hotel_order_simulator.py