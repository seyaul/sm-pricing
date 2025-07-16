# models_pricing/__init__.py
import os
import re
from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, DateTime, ForeignKey
)
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

load_dotenv()

print("DEBUG: DATABASE_URL =", repr(os.environ.get("DATABASE_URL")))
url = os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1)
print("DEBUG: DATABASE_URL=", url)
engine = create_engine(url, pool_pre_ping=True, echo=False)
# --------------------------------------------------------------------------- #
# Database engine & base
# --------------------------------------------------------------------------- #
# engine = create_engine(
#     os.environ["DATABASE_URL"], pool_pre_ping=True, echo=False
# )
Base = declarative_base()


# --------------------------------------------------------------------------- #
# Helper: normalise UPC / GTIN to 12-digit string
# --------------------------------------------------------------------------- #
_UPC_DIGITS = re.compile(r"\D")

def clean_upc(raw: str | int) -> str:
    """Return a zero-padded 12-digit UPC string."""
    digits = _UPC_DIGITS.sub("", str(raw))
    # keep last 12 digits (dropping any GTIN-14 prefix) and pad if short
    return digits.zfill(12)[-12:]


# --------------------------------------------------------------------------- #
# Canonical (clean) tables
# --------------------------------------------------------------------------- #
class Product(Base):
    __tablename__ = "products"

    sku       = Column(String, primary_key=True)   # use cleaned 12-digit UPC
    brand     = Column(String)
    category  = Column(String)
    item_name = Column(String)
    size      = Column(String)


class Movement(Base):
    __tablename__ = "movement"

    id         = Column(Integer, primary_key=True)
    sku        = Column(ForeignKey("products.sku"))
    units_sold = Column(Integer)
    avg_price  = Column(Numeric(10, 2))
    cycle_tag  = Column(String, index=True)        # e.g. "2025-02"


class VendorCost(Base):
    __tablename__ = "vendor_cost"

    id        = Column(Integer, primary_key=True)
    sku       = Column(ForeignKey("products.sku"))
    vendor_id = Column(String, index=True)
    cost      = Column(Numeric(10, 2))
    cycle_tag = Column(String, index=True)


class PriceChangeLog(Base):
    __tablename__ = "price_change_log"

    id             = Column(Integer, primary_key=True)
    sku            = Column(ForeignKey("products.sku"))
    new_price      = Column(Numeric(10, 2))
    effective_date = Column(DateTime)


class PriceProposal(Base):
    __tablename__ = "price_proposal"

    id           = Column(Integer, primary_key=True)
    sku          = Column(ForeignKey("products.sku"))
    cycle_tag    = Column(String, index=True)
    auto_price   = Column(Numeric(10, 2))
    edited_price = Column(Numeric(10, 2), nullable=True)
    status       = Column(String, default="pending")  # pending / approved / pushed


# --------------------------------------------------------------------------- #
# Staging tables (raw ingests)
# --------------------------------------------------------------------------- #
class MovementStaging(Base):
    __tablename__ = "movement_staging"

    id         = Column(Integer, primary_key=True)
    upc_clean  = Column(String, index=True)
    brand      = Column(String)
    category   = Column(String)
    item_name  = Column(String)
    avg_price  = Column(Numeric(10, 2))
    units_sold = Column(Integer)
    cycle_tag  = Column(String, index=True)


class VendorCostStaging(Base):
    __tablename__ = "vendor_cost_staging"

    id        = Column(Integer, primary_key=True)
    upc_clean = Column(String, index=True)
    vendor_id = Column(String, index=True)
    cost      = Column(Numeric(10, 2))
    cycle_tag = Column(String, index=True)


# --------------------------------------------------------------------------- #
# CLI helper: create tables when run as module
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print("✅ Tables created (or already exist).")
