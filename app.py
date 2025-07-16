from fastapi import FastAPI
from models_pricing import Base, engine
import os
from sqlalchemy import text
from sqlalchemy import inspect
import uvicorn

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "healthy", "database": "connected"}

@app.get("/create-tables")
def create_tables():
    try:
        Base.metadata.create_all(engine)
        return {"status": "success", "message": "Tables created"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@app.get("/tables")
def list_tables():
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        table_info = {}
        for table in tables:
            columns = [col['name'] for col in inspector.get_columns(table)]
            table_info[table] = columns
            
        return {"tables": table_info, "count": len(tables)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/table-count")
def count_records():
    try:
        with engine.connect() as conn:
            counts = {}
            inspector = inspect(engine)
            for table in inspector.get_table_names():
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = result.scalar()
        return {"table_counts": counts}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/add-test-data")
def add_test_data():
    try:
        from models_pricing import engine, Product
        from sqlalchemy.orm import sessionmaker
        import random
        
        # Create session manually
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        # Generate random SKU to avoid duplicates
        random_sku = f"TEST{random.randint(100000, 999999)}"
        
        # Add a test product
        test_product = Product(
            sku=random_sku,
            brand="Test Brand",
            category="Test Category", 
            item_name="Test Product",
            size="1 EA"
        )
        session.add(test_product)
        session.commit()
        session.close()
        
        return {"status": "success", "message": f"Test data added with SKU: {random_sku}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/clear-tables")
def clear_tables():
    try:
        from models_pricing import Base, engine
        from sqlalchemy.orm import sessionmaker
        
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        # Get all table names
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        # Clear each table
        cleared_tables = []
        for table_name in tables:
            session.execute(text(f"DELETE FROM {table_name}"))
            cleared_tables.append(table_name)
        
        session.commit()
        session.close()
        
        return {"status": "success", "message": f"Cleared tables: {cleared_tables}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@app.get("/table/{table_name}/data")
def get_table_data(table_name: str):
    try:
        # Validate table exists
        inspector = inspect(engine)
        available_tables = inspector.get_table_names()
        
        if table_name not in available_tables:
            return {
                "status": "error", 
                "message": f"Table '{table_name}' not found. Available tables: {available_tables}"
            }
        
        # Get table data
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name}"))
            
            # Get column names
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            
            # Convert rows to list of dictionaries
            rows = []
            for row in result:
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[columns[i]] = value
                rows.append(row_dict)
        
        return {
            "table": table_name,
            "columns": columns,
            "row_count": len(rows),
            "data": rows
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/table-data")
def get_table_data_query(table: str):
    """Alternative endpoint using query parameter: /table-data?table=products"""
    try:
        # Validate table exists
        inspector = inspect(engine)
        available_tables = inspector.get_table_names()
        
        if table not in available_tables:
            return {
                "status": "error", 
                "message": f"Table '{table}' not found. Available tables: {available_tables}"
            }
        
        # Get table data
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table}"))
            
            # Get column names
            columns = [col['name'] for col in inspector.get_columns(table)]
            
            # Convert rows to list of dictionaries
            rows = []
            for row in result:
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[columns[i]] = value
                rows.append(row_dict)
        
        return {
            "table": table,
            "columns": columns,
            "row_count": len(rows),
            "data": rows
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)