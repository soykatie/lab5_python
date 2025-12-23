from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
import csv
import aiohttp
import asyncio
from datetime import timedelta
from sqlalchemy import desc


DATABASE_URL = "sqlite:///./cities.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class City(Base):
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    temperature = Column(Float, nullable=True)
    updated_at = Column(DateTime, nullable=True)


class DefaultCity(Base):
    __tablename__ = "default_cities"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)


Base.metadata.create_all(bind=engine)

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="styles"), name="static")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def fetch_weather(latitude: float, longitude: float):
    async with aiohttp.ClientSession() as session:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        async with session.get(url) as response:
            data = await response.json()
            return data["current_weather"]["temperature"]


async def update_city_weather(city):
    temp = await fetch_weather(city.latitude, city.longitude)
    return city, temp


# Routes
@app.get("/")
async def read_root(request: Request, db: SessionLocal = Depends(get_db)):
    cities = (
        db.query(City)
        .order_by(desc(City.temperature.is_(None)), desc(City.temperature))
        .all()
    )

    message = request.query_params.get("message")
    msg_type = request.query_params.get("type", "error")

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "cities": cities,
            "message": message,
            "type": msg_type
        }
    )


@app.post("/cities/add")
async def add_city(
    name: str = Form(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    db: SessionLocal = Depends(get_db)
):
    existing = db.query(City).filter(City.name.ilike(name)).first()
    if existing:
        return RedirectResponse(
            "/?message=This city already exists&type=error",
            status_code=303
        )

    city = City(name=name, latitude=latitude, longitude=longitude)
    db.add(city)
    db.commit()
    return RedirectResponse(
        "/?message=City added&type=success",
        status_code=303
    )


@app.post("/cities/remove/{city_id}")
async def remove_city(city_id: int, db: SessionLocal = Depends(get_db)):
    city = db.query(City).filter(City.id == city_id).first()
    if city:
        db.delete(city)
        db.commit()
    return RedirectResponse(
        "/?message=City deleted&type=success",
        status_code=303
    )


@app.post("/cities/reset")
async def reset_cities(db: SessionLocal = Depends(get_db)):
    db.query(City).delete()

    default_cities = db.query(DefaultCity).all()
    for default in default_cities:
        db.add(
            City(
                name=default.name,
                latitude=default.latitude,
                longitude=default.longitude
            )
        )
    db.commit()

    return RedirectResponse(
        "/?message=City list reset&type=success",
        status_code=303
    )


@app.post("/cities/update")
async def update_weather(db: SessionLocal = Depends(get_db)):
    cities = db.query(City).all()
    now = datetime.utcnow()

    tasks = []
    for city in cities:
        if city.updated_at and now - city.updated_at < timedelta(minutes=15):
            continue
        tasks.append(update_city_weather(city))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            continue
        city, temp = result
        city.temperature = temp
        city.updated_at = now

    db.commit()
    return RedirectResponse(
        "/?message=Temperature reset&type=success",
        status_code=303
    )


@app.on_event("startup")
def populate_default_cities():
    db = SessionLocal()
    if not db.query(DefaultCity).first():
        with open("europe.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                db.add(DefaultCity(name=row["name"], latitude=float(row["latitude"]), longitude=float(row["longitude"])))
        db.commit()
    db.close()