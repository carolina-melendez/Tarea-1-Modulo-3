/* =========================
   Creando la base de datos
   ========================= */
IF DB_ID('DWTailspinToys') IS NULL
    CREATE DATABASE DWTailspinToys;
GO
USE DWTailspinToys;
GO

/* =========================
   Dimensiones
   ========================= */

/* --- dim_tiempo (dinámica + mínimo 5 años) --- */
IF OBJECT_ID('dim_tiempo','U') IS NOT NULL DROP TABLE dim_tiempo;
GO
CREATE TABLE dim_tiempo(
  fecha_key        INT          NOT NULL PRIMARY KEY,   -- yyyymmdd
  fecha_completa   DATE         NOT NULL,
  nro_dia_semana   TINYINT      NOT NULL,
  nro_dia_mes      TINYINT      NOT NULL,
  nro_semana       TINYINT      NOT NULL,
  nro_mes          TINYINT      NOT NULL,
  nro_trimestre    TINYINT      NOT NULL,
  anio             INT          NOT NULL,
  es_fin_de_semana BIT          NOT NULL,
  nombre_dia       NVARCHAR(20) NOT NULL,
  nombre_mes       NVARCHAR(20) NOT NULL
);
GO

DECLARE 
    @fecha_min_src DATE,
    @fecha_max_src DATE,
    @fecha_min DATE,
    @fecha_max DATE,
    @years INT,
    @to_add INT;

-- Rango real desde el OLTP
SELECT 
    @fecha_min_src = MIN(OrderDate),
    @fecha_max_src = MAX(COALESCE(ShipDate, OrderDate))
FROM [TailspinToys2020-US].dbo.Sales;

-- Extiende 1 año a cada lado
SET @fecha_min = DATEFROMPARTS(YEAR(@fecha_min_src)-1, 1, 1);
SET @fecha_max = DATEFROMPARTS(YEAR(@fecha_max_src)+1, 12, 31);

SET @years = DATEDIFF(YEAR, @fecha_min, @fecha_max) + 1;
IF (@years < 5)
BEGIN
    SET @to_add = 5 - @years;
    SET @fecha_min = DATEFROMPARTS(YEAR(DATEADD(YEAR, -CEILING(@to_add/2.0), @fecha_min)), 1, 1);
    SET @fecha_max = DATEFROMPARTS(YEAR(DATEADD(YEAR,  FLOOR(@to_add/2.0), @fecha_max)), 12, 31);
END

;WITH d AS(
  SELECT @fecha_min AS dt
  UNION ALL
  SELECT DATEADD(DAY,1,dt) FROM d WHERE dt < @fecha_max
)
INSERT INTO dim_tiempo
SELECT
  CONVERT(INT, FORMAT(dt,'yyyyMMdd'))         AS fecha_key,
  dt                                          AS fecha_completa,
  DATEPART(WEEKDAY,dt)                        AS nro_dia_semana,
  DAY(dt)                                     AS nro_dia_mes,
  DATEPART(WEEK,dt)                           AS nro_semana,
  MONTH(dt)                                   AS nro_mes,
  DATEPART(QUARTER,dt)                        AS nro_trimestre,
  YEAR(dt)                                    AS anio,
  CASE WHEN DATEPART(WEEKDAY,dt) IN (1,7) THEN 1 ELSE 0 END AS es_fin_de_semana,
  DATENAME(WEEKDAY,dt)                        AS nombre_dia,
  DATENAME(MONTH,dt)                          AS nombre_mes
FROM d
OPTION (MAXRECURSION 0);
GO

/* --- dim_producto--- */
IF OBJECT_ID('dim_producto','U') IS NOT NULL DROP TABLE dim_producto;
GO
CREATE TABLE dim_producto(
  id_producto_sk      BIGINT IDENTITY(1,1) PRIMARY KEY,
  id_producto_bk      INT            NOT NULL,           -- BK del OLTP
  vigente_desde       DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME(),
  vigente_hasta       DATETIME2(3)   NOT NULL DEFAULT '9999-12-31',
  es_actual           BIT            NOT NULL DEFAULT 1,
  sku_producto        NVARCHAR(50)   NULL,
  nombre_producto     NVARCHAR(200)  NULL,
  categoria_producto  NVARCHAR(100)  NULL,
  grupo_item          NVARCHAR(100)  NULL,
  precio_retail       DECIMAL(18,4)  NULL,
  hash_diff           VARBINARY(32)  NULL,
  CONSTRAINT uq_dim_producto UNIQUE(id_producto_bk, vigente_hasta)
);
CREATE INDEX ix_dim_producto_lookup 
  ON dim_producto(id_producto_bk, es_actual) INCLUDE(hash_diff);

/* --- dim_estado_client--- */
IF OBJECT_ID('dim_estado_cliente','U') IS NOT NULL DROP TABLE dim_estado_cliente;
GO
CREATE TABLE dim_estado_cliente(
  id_estado_cliente_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
  id_region            INT            NULL,
  vigente_desde        DATETIME2(3)   NOT NULL DEFAULT SYSDATETIME(),
  vigente_hasta        DATETIME2(3)   NOT NULL DEFAULT '9999-12-31',
  es_actual            BIT            NOT NULL DEFAULT 1,
  id_estado_cliente_bk INT            NOT NULL,          -- BK (StateID)
  codigo_estado        NVARCHAR(10)   NULL,
  nombre_estado        NVARCHAR(100)  NULL,
  zona_horaria         NVARCHAR(50)   NULL,
  nombre_region        NVARCHAR(100)  NULL,
  hash_diff            VARBINARY(32)  NULL,
  CONSTRAINT uq_dim_estado_cliente UNIQUE(id_estado_cliente_bk, vigente_hasta)
);
CREATE INDEX ix_dim_estado_cliente_lookup 
  ON dim_estado_cliente(id_estado_cliente_bk, es_actual) INCLUDE(hash_diff);

/* --- dim_basura--- */
IF OBJECT_ID('dim_basura','U') IS NOT NULL DROP TABLE dim_basura;
GO
CREATE TABLE dim_basura(
  id_basura_sk     BIGINT IDENTITY(1,1) PRIMARY KEY,
  canal            NVARCHAR(100) NULL,   -- Product.Channels
  tipo_kit         NVARCHAR(100) NULL,   -- Product.KitType
  demografico      NVARCHAR(100) NULL,   -- Product.Demographic
  codigo_promocion NVARCHAR(50)  NULL,   -- Sales.PromotionCode
  CONSTRAINT uq_dim_basura UNIQUE(canal, tipo_kit, demografico, codigo_promocion)
);
GO

/* =========================
   Tabla de Hechos
   ========================= */
IF OBJECT_ID('hecho_venta_linea','U') IS NOT NULL DROP TABLE hecho_venta_linea;
GO
CREATE TABLE hecho_venta_linea(
  -- FKs a dimensiones
  fecha_pedido_key      INT        NOT NULL,
  fecha_envio_key       INT        NULL,
  id_producto_sk        BIGINT     NOT NULL,
  id_estado_cliente_sk  BIGINT     NOT NULL,
  id_basura_sk          BIGINT     NOT NULL,

  load_batch_id         INT         NULL,
  insertado_en          DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(),
  numero_orden          NVARCHAR(50) NULL,

  cantidad              DECIMAL(18,4) NOT NULL,
  precio_unitario       DECIMAL(18,4) NOT NULL,
  descuento_monto       DECIMAL(18,4) NOT NULL,
  importe_extendido     AS (cantidad * precio_unitario - descuento_monto) PERSISTED,

  CONSTRAINT fk_fact_fecha_pedido  FOREIGN KEY(fecha_pedido_key)     REFERENCES dim_tiempo(fecha_key),
  CONSTRAINT fk_fact_fecha_envio   FOREIGN KEY(fecha_envio_key)      REFERENCES dim_tiempo(fecha_key),
  CONSTRAINT fk_fact_producto      FOREIGN KEY(id_producto_sk)       REFERENCES dim_producto(id_producto_sk),
  CONSTRAINT fk_fact_estado_cli    FOREIGN KEY(id_estado_cliente_sk) REFERENCES dim_estado_cliente(id_estado_cliente_sk),
  CONSTRAINT fk_fact_basura        FOREIGN KEY(id_basura_sk)         REFERENCES dim_basura(id_basura_sk)
);
CREATE INDEX ix_fact_fecha_pedido       ON hecho_venta_linea(fecha_pedido_key);
CREATE INDEX ix_fact_producto           ON hecho_venta_linea(id_producto_sk);
CREATE INDEX ix_fact_estado_cliente     ON hecho_venta_linea(id_estado_cliente_sk);
CREATE INDEX ix_fact_basura             ON hecho_venta_linea(id_basura_sk);
GO
