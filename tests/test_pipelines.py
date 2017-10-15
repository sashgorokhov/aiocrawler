import pytest

from aiocrawler.exceptions import DropItem
from aiocrawler.pipelines import ItemPipelineManager
from tests.utils import MiddlewareMethodGenerator


@pytest.fixture()
def pipeline_manager(engine):
    return ItemPipelineManager.from_engine(engine)


@pytest.fixture()
def item():
    return {'title': 'Test item'}


@pytest.mark.asyncio
@pytest.mark.parametrize('pipelines', [
    [{'kwargs': {'return_value': {'title': 'Test item'}}}, {'kwargs': {'side_effect': DropItem()}}, {'called': False}]
])
async def test_process_item_dropped(pipelines, pipeline_manager: ItemPipelineManager, spider, item):
    methodgen = MiddlewareMethodGenerator(pipelines)
    methodgen.generate('process_item', pipeline_manager)

    assert await pipeline_manager.process_item(spider, item) is None

    methodgen.assert_methods(None, check_results=False)


@pytest.mark.asyncio
@pytest.mark.parametrize('pipelines', [
    [{'kwargs': {'return_value': {'title': 'Test item'}}}, {'kwargs': {'side_effect': ValueError()}}, {'called': False}]
])
async def test_process_item_error(pipelines, pipeline_manager: ItemPipelineManager, spider, item):
    methodgen = MiddlewareMethodGenerator(pipelines)
    methodgen.generate('process_item', pipeline_manager)

    assert await pipeline_manager.process_item(spider, item) is None

    methodgen.assert_methods(None, check_results=False)


@pytest.mark.asyncio
@pytest.mark.parametrize('pipelines', [
    [{}, {}, {}],
    [{}, {'kwargs': {'return_value': {'title': 'Test item 2'}}}, {}],
])
async def test_process_item(pipelines, pipeline_manager: ItemPipelineManager, spider, item):
    methodgen = MiddlewareMethodGenerator(pipelines)
    methodgen.generate('process_item', pipeline_manager)
    for d in methodgen.methodspec:
        if not d['mock'].return_value:
            d['mock'].return_value = item
            d['kwargs']['return_value'] = item

    assert await pipeline_manager.process_item(spider, item) == {'title': 'Test item 2'}

    methodgen.assert_methods(item, check_results=False)
