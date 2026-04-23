"""Master product catalog shared by all platform simulators.

Each product has a canonical SKU plus platform-specific SKU variants.
This deliberately mirrors the real-world pain: the same product has
5 different codes across 5 platforms.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Product:
    sku_canonical: str
    name: str
    category: str
    base_price_usd: float
    # Platform-specific SKU variants (messy on purpose)
    sku_shopify: str
    sku_mercadolibre: str
    sku_amazon: str
    sku_tiendanube: str
    sku_pos: str


CATALOG: list[Product] = [
    Product("POL-001", "Polo Basico Negro M", "Polos", 19.90,
            "SHP-POL001-BLK-M", "MLA-polo-basico-negro-m", "B0POL001BKM",
            "polo-basico-negro-m", "POS0001"),
    Product("POL-002", "Polo Basico Blanco M", "Polos", 19.90,
            "SHP-POL002-WHT-M", "MLA-polo-basico-blanco-m", "B0POL002WHM",
            "polo-basico-blanco-m", "POS0002"),
    Product("POL-003", "Polo Estampado Azul L", "Polos", 24.90,
            "SHP-POL003-BLU-L", "MLA-polo-estampado-azul-l", "B0POL003BLL",
            "polo-estampado-azul-l", "POS0003"),
    Product("POL-004", "Polo Deportivo Rojo M", "Polos", 29.90,
            "SHP-POL004-RED-M", "MLA-polo-deportivo-rojo-m", "B0POL004RDM",
            "polo-deportivo-rojo-m", "POS0004"),
    Product("POL-005", "Polo Deportivo Azul L", "Polos", 29.90,
            "SHP-POL005-BLU-L", "MLA-polo-deportivo-azul-l", "B0POL005BLL",
            "polo-deportivo-azul-l", "POS0005"),
    Product("CAM-001", "Camisa Oxford Azul M", "Camisas", 49.90,
            "SHP-CAM001-BLU-M", "MLA-camisa-oxford-azul-m", "B0CAM001BLM",
            "camisa-oxford-azul-m", "POS0010"),
    Product("CAM-002", "Camisa Oxford Blanca L", "Camisas", 49.90,
            "SHP-CAM002-WHT-L", "MLA-camisa-oxford-blanca-l", "B0CAM002WHL",
            "camisa-oxford-blanca-l", "POS0011"),
    Product("CAM-003", "Camisa Lino Beige M", "Camisas", 54.90,
            "SHP-CAM003-BEI-M", "MLA-camisa-lino-beige-m", "B0CAM003BEM",
            "camisa-lino-beige-m", "POS0012"),
    Product("PAN-001", "Pantalon Chino Beige 32", "Pantalones", 59.90,
            "SHP-PAN001-BEI-32", "MLA-pantalon-chino-beige-32", "B0PAN001BE32",
            "pantalon-chino-beige-32", "POS0020"),
    Product("PAN-002", "Pantalon Chino Negro 32", "Pantalones", 59.90,
            "SHP-PAN002-BLK-32", "MLA-pantalon-chino-negro-32", "B0PAN002BK32",
            "pantalon-chino-negro-32", "POS0021"),
    Product("PAN-003", "Jean Slim Azul 30", "Pantalones", 69.90,
            "SHP-PAN003-BLU-30", "MLA-jean-slim-azul-30", "B0PAN003BL30",
            "jean-slim-azul-30", "POS0022"),
    Product("PAN-004", "Short Deportivo Negro M", "Pantalones", 34.90,
            "SHP-PAN004-BLK-M", "MLA-short-deportivo-negro-m", "B0PAN004BKM",
            "short-deportivo-negro-m", "POS0023"),
    Product("PAN-005", "Bermuda Cargo Verde 32", "Pantalones", 44.90,
            "SHP-PAN005-GRN-32", "MLA-bermuda-cargo-verde-32", "B0PAN005GR32",
            "bermuda-cargo-verde-32", "POS0024"),
    Product("CAS-001", "Casaca Cuero Negra L", "Casacas", 149.90,
            "SHP-CAS001-BLK-L", "MLA-casaca-cuero-negra-l", "B0CAS001BKL",
            "casaca-cuero-negra-l", "POS0030"),
    Product("CAS-002", "Casaca Jean Azul M", "Casacas", 89.90,
            "SHP-CAS002-BLU-M", "MLA-casaca-jean-azul-m", "B0CAS002BLM",
            "casaca-jean-azul-m", "POS0031"),
    Product("CAS-003", "Hoodie Gris L", "Casacas", 64.90,
            "SHP-CAS003-GRY-L", "MLA-hoodie-gris-l", "B0CAS003GRL",
            "hoodie-gris-l", "POS0032"),
    Product("CAS-004", "Chompa Lana Negra M", "Casacas", 79.90,
            "SHP-CAS004-BLK-M", "MLA-chompa-lana-negra-m", "B0CAS004BKM",
            "chompa-lana-negra-m", "POS0033"),
    Product("ZAP-001", "Zapatilla Urbana Blanca 42", "Calzado", 79.90,
            "SHP-ZAP001-WHT-42", "MLA-zapatilla-urbana-blanca-42", "B0ZAP001WH42",
            "zapatilla-urbana-blanca-42", "POS0040"),
    Product("ZAP-002", "Zapatilla Running Negra 41", "Calzado", 99.90,
            "SHP-ZAP002-BLK-41", "MLA-zapatilla-running-negra-41", "B0ZAP002BK41",
            "zapatilla-running-negra-41", "POS0041"),
    Product("ZAP-003", "Mocasin Marron 42", "Calzado", 89.90,
            "SHP-ZAP003-BRN-42", "MLA-mocasin-marron-42", "B0ZAP003BR42",
            "mocasin-marron-42", "POS0042"),
    Product("ZAP-004", "Sandalia Playa Negra 42", "Calzado", 24.90,
            "SHP-ZAP004-BLK-42", "MLA-sandalia-playa-negra-42", "B0ZAP004BK42",
            "sandalia-playa-negra-42", "POS0043"),
    Product("ZAP-005", "Bota Trekking Marron 43", "Calzado", 129.90,
            "SHP-ZAP005-BRN-43", "MLA-bota-trekking-marron-43", "B0ZAP005BR43",
            "bota-trekking-marron-43", "POS0044"),
    Product("ACC-001", "Correa Cuero Negra", "Accesorios", 29.90,
            "SHP-ACC001-BLK", "MLA-correa-cuero-negra", "B0ACC001BK",
            "correa-cuero-negra", "POS0050"),
    Product("ACC-002", "Billetera Cuero Marron", "Accesorios", 39.90,
            "SHP-ACC002-BRN", "MLA-billetera-cuero-marron", "B0ACC002BR",
            "billetera-cuero-marron", "POS0051"),
    Product("ACC-003", "Gorra Logo Negra", "Accesorios", 19.90,
            "SHP-ACC003-BLK", "MLA-gorra-logo-negra", "B0ACC003BK",
            "gorra-logo-negra", "POS0052"),
    Product("ACC-004", "Mochila Urbana Gris", "Accesorios", 59.90,
            "SHP-ACC004-GRY", "MLA-mochila-urbana-gris", "B0ACC004GR",
            "mochila-urbana-gris", "POS0053"),
    Product("ACC-005", "Lentes Sol Aviador", "Accesorios", 49.90,
            "SHP-ACC005", "MLA-lentes-sol-aviador", "B0ACC005AV",
            "lentes-sol-aviador", "POS0054"),
    Product("ACC-006", "Reloj Deportivo Negro", "Accesorios", 89.90,
            "SHP-ACC006-BLK", "MLA-reloj-deportivo-negro", "B0ACC006BK",
            "reloj-deportivo-negro", "POS0055"),
    Product("ACC-007", "Bufanda Lana Gris", "Accesorios", 24.90,
            "SHP-ACC007-GRY", "MLA-bufanda-lana-gris", "B0ACC007GR",
            "bufanda-lana-gris", "POS0056"),
    Product("ACC-008", "Guantes Cuero Negros", "Accesorios", 34.90,
            "SHP-ACC008-BLK", "MLA-guantes-cuero-negros", "B0ACC008BK",
            "guantes-cuero-negros", "POS0057"),
]


def get_products_for_platform(platform: str) -> list[dict]:
    """Return products with the SKU field named for the target platform."""
    platform_map = {
        "shopify": "sku_shopify",
        "mercadolibre": "sku_mercadolibre",
        "amazon": "sku_amazon",
        "tiendanube": "sku_tiendanube",
        "pos": "sku_pos",
    }
    if platform not in platform_map:
        raise ValueError(f"Unknown platform: {platform}")

    sku_field = platform_map[platform]
    return [
        {
            "sku": getattr(p, sku_field),
            "name": p.name,
            "category": p.category,
            "base_price_usd": p.base_price_usd,
            "sku_canonical": p.sku_canonical,
        }
        for p in CATALOG
    ]
