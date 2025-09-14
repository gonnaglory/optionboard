from fastapi import FastAPI, HTTPException, Request, Response
import ujson, aiofiles
from contextlib import asynccontextmanager

from .http_client import MOEXClient
from .models import OptionData
from .config import settings 

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Создаём клиент при старте
    app.state.moex_client = MOEXClient(base_url='https://iss.moex.com')
    try:
        # Загружаем данные опционов при запуске
        await app.state.moex_client.get_options()
        async with aiofiles.open(settings.DATA_FOLDER / "UNDERLYINGASSETS.json", "r", encoding="utf-8") as f:
            assets = ujson.loads(await f.read())
            for asset in assets:
                await app.state.moex_client.load_candles(asset)
                await app.state.moex_client.add_params(asset)

        
        yield
    finally:
        # Закрываем соединение при завершении
        await app.state.moex_client.close()


app = FastAPI(lifespan=lifespan)


@app.get('/')
async def root(request: Request):
    """Root endpoint: показывает доступные базовые активы."""
    moex_client = request.app.state.moex_client
    try:
        options = await moex_client.get_options()
        return {"message": "OptionBoard API is running", "available_options": options}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/favicon.ico")
async def favicon():
    # возвращаем пустую иконку или ничего
    return Response(status_code=204)


@app.get('/{asset}')
async def get_option(asset: str):
    
    # file_path = settings.DATA_FOLDER / f"{asset}.json"

    # await app.state.moex_client.get_options()
    await app.state.moex_client.load_candles(asset)

    await app.state.moex_client.add_params(asset)
    
    # try:
    #     with open(file_path, 'r', encoding='utf-8') as file:
    #         json_data = ujson.load(file)
    #         return json_data

    # except ujson.JSONDecodeError as e:
    #     raise HTTPException(
    #         status_code=500,
    #         detail=f"Invalid JSON data in {asset}.json: {str(e)}"
    #     )
    # except ValidationError as e:
    #     raise HTTPException(
    #         status_code=500,
    #         detail=f"Data validation error: {str(e)}"
    #     )
    # except Exception as e:
    #     raise HTTPException(
    #         status_code=500,
    #         detail=f"Error processing request: {str(e)}"
    #     )
    
# if __name__ == '__main__':
#     uvicorn src.main:app --reload