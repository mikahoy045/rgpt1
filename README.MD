
RUN TEST DATA PROVIDER
pytest tests/test_dprovider.py

RUN SERVICE
uvicorn dataprovider:app --reload

RUN HOTEL ORDER SIMULATION 
python Simulator/hotel_order_simulator.py