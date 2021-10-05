use warehouse COMPUTE_WH;

Select * from FIVETRAN_DB.PROD.INVENTORY_FACT;

Select * from FIVETRAN_DB.PROD.ITEM_DIM
where source = 'Lightspeed'