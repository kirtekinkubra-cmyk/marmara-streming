"""
common/facility_ranking.py

Bu modul, 09_facility_ranking.py dosyanizdaki tesis hazirlama/skorlama
fonksiyonlarinin degistirilmemis halidir; sadece dosya I/O ve CLI (main())
kismi kaldirildi. facility_logistics_batch.py bu fonksiyonlari import edip
sadece ETKILENEN event_id icin cagirir -- tum senaryo setini degil.
"""

from pathlib import Path
import re
import unicodedata

import numpy as np
import pandas as pd


# Yol sabitleri kaldırıldı — bu modül artık streaming job içinden çağrılan
# saf fonksiyon kütüphanesi. Girdi/çıktı Job 3 (facility_logistics_batch.py)
# tarafından yönetilir.


# =========================================================
# MODEL CONFIGURATION
# =========================================================

# Ana lojistik aktivasyon merkezi olabilecek tesis türleri
CANDIDATE_CATEGORIES = {
    "havalimani",
    "liman",
    "feribot_iskele",
    "otogar",
}


# Ana merkezin çevresinde operasyonu destekleyen noktalar
SUPPORT_CATEGORIES = {
    "hastane",
    "itfaiye",
    "polis",
    "akaryakit",
}


FACILITY_TYPE_SCORES = {
    "havalimani": 100,
    "liman": 90,
    "feribot_iskele": 70,
    "otogar": 65,
}


SUPPORT_WEIGHTS = {
    "hastane": 3,
    "itfaiye": 2,
    "akaryakit": 2,
    "polis": 1,
}


# Otogar kategorisine yanlış atanmış şehir içi ulaşım noktaları
EXCLUDED_OTOGAR_KEYWORDS = {
    "peron",
    "durak",
    "basduragi",
    "mahalle",
    "mah",
    "hastane",
    "metro",
    "minibus",
    "dolmus",
    "toplu tasima",
    "iett",
    "otobus duragi",
    "servis duragi",
    "terminal peron",
    "peron",
    "durak",
    "basduragi",
    "mahalle",
    "hastane",
    "metro",
    "minibus",
    "dolmus",
    "toplu tasima",
    "iett",
    "servis",
}


MILITARY_KEYWORDS = {
    "ana us",
    "hava ussu",
    "jet us",
    "komutanligi",
    "askeri",
}


PRIVATE_OR_SPECIAL_KEYWORDS = {
    "deniz ucagi",
    "seabird",
    "hara havaalani",
}


MAX_PRIORITY_DISTANCE_KM = 100
LOCAL_SUPPORT_RADIUS_KM = 15
MULTIMODAL_RADIUS_KM = 20


ACTIVATION_WEIGHTS = {
    "access": 0.50,
    "safety": 0.25,
    "facility_type": 0.15,
    "multimodal": 0.05,
    "local_support": 0.05,
}


# =========================================================
# TEXT FUNCTIONS
# =========================================================

def normalize_text(value):
    """
    Metni karşılaştırma ve filtreleme için sadeleştirir.
    """

    if pd.isna(value):
        return ""

    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKC", text)

    replacements = {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
        "â": "a",
        "î": "i",
        "û": "u",
    }

    for source, target in replacements.items():
        text = text.replace(source, target)

    text = re.sub(
        r"[^a-z0-9\s]",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    return text


def contains_excluded_otogar_keyword(name_key):
    """
    Otogar kategorisine yanlış atanmış şehir içi ulaşım,
    durak, peron ve hastane kayıtlarını belirler.
    """

    return any(
        keyword in name_key
        for keyword in EXCLUDED_OTOGAR_KEYWORDS
    )

def classify_facility_access_type(
    facility_name,
    category,
):
    """
    Tesisin kullanım niteliğini temel isim bilgisine göre sınıflandırır.

    Bu sınıflandırma, tesisi doğrudan elemek için değil,
    sonuçların yorumlanmasını desteklemek için kullanılır.
    """

    name_key = normalize_text(
        facility_name
    )

    if any(
        keyword in name_key
        for keyword in MILITARY_KEYWORDS
    ):
        return "military"

    if any(
        keyword in name_key
        for keyword in PRIVATE_OR_SPECIAL_KEYWORDS
    ):
        return "special_or_private"

    if category in {
        "havalimani",
        "liman",
        "feribot_iskele",
        "otogar",
    }:
        return "civilian_or_unknown"

    return "unknown"


# =========================================================
# GENERAL FUNCTIONS
# =========================================================

def haversine_distance(
    lat1,
    lon1,
    lat2,
    lon2,
):
    """
    İki koordinat arasındaki kuş uçuşu mesafeyi
    kilometre cinsinden hesaplar.
    """

    earth_radius_km = 6371.0

    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    a = (
        np.sin(delta_lat / 2) ** 2
        + np.cos(lat1_rad)
        * np.cos(lat2_rad)
        * np.sin(delta_lon / 2) ** 2
    )

    c = 2 * np.arctan2(
        np.sqrt(a),
        np.sqrt(1 - a),
    )

    return earth_radius_km * c


def min_max_scale(series):
    """
    Sayısal değerleri 0-100 aralığına dönüştürür.
    """

    clean_series = pd.to_numeric(
        series,
        errors="coerce",
    ).fillna(0)

    minimum = clean_series.min()
    maximum = clean_series.max()

    if maximum == minimum:
        return pd.Series(
            np.zeros(len(clean_series)),
            index=clean_series.index,
        )

    return (
        (clean_series - minimum)
        / (maximum - minimum)
        * 100
    )


def calculate_event_strength(
    magnitude,
    depth_km,
):
    """
    08_event_impact.py ile aynı büyüklük ve
    derinlik yaklaşımını kullanır.
    """

    magnitude_score = np.clip(
        (
            (magnitude - 5.0)
            / (7.5 - 5.0)
        )
        * 100,
        0,
        100,
    )

    depth_score = np.clip(
        100
        - (
            depth_km
            / 40.0
            * 100
        ),
        0,
        100,
    )

    return (
        magnitude_score * 0.70
        + depth_score * 0.30
    )


def calculate_facility_event_impact(
    facilities,
    event_latitude,
    event_longitude,
    magnitude,
    depth_km,
):
    """
    Deprem olayının aday tesis üzerindeki
    doğrudan etkisini hesaplar.
    """

    distance_to_event = haversine_distance(
        facilities["latitude"],
        facilities["longitude"],
        event_latitude,
        event_longitude,
    )

    distance_score = (
        100
        * np.exp(
            -distance_to_event
            / 75.0
        )
    )

    event_strength = (
        calculate_event_strength(
            magnitude,
            depth_km,
        )
    )

    event_impact = (
        event_strength
        * distance_score
        / 100
    )

    return (
        distance_to_event,
        np.clip(
            event_impact,
            0,
            100,
        ),
    )


# =========================================================
# FACILITY PREPARATION
# =========================================================

def prepare_facilities(
    osm,
    risk,
):
    """
    Ana tesis adaylarını seçer, kirli otogar kayıtlarını
    ayıklar, tekrarları azaltır ve risk bilgilerini ekler.
    """

    candidates = osm[
        osm["kategori"].isin(
            CANDIDATE_CATEGORIES
        )
    ].copy()

    candidates = candidates.dropna(
        subset=[
            "latitude",
            "longitude",
        ]
    ).copy()

    candidates["facility_name"] = (
        candidates["isim"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    candidates["facility_name_key"] = (
        candidates["facility_name"]
        .apply(normalize_text)
    )

    # -----------------------------------------------------
    # Kirli otogar kayıtlarının temizlenmesi
    # -----------------------------------------------------

    is_otogar = (
        candidates["kategori"]
        == "otogar"
    )

    excluded_otogar = (
        candidates["facility_name_key"]
        .apply(
            contains_excluded_otogar_keyword
        )
    )

    candidates = candidates.loc[
        ~(
            is_otogar
            & excluded_otogar
        )
    ].copy()

    candidates = (
        candidates
        .reset_index(drop=True)
    )

    # -----------------------------------------------------
    # İsimsiz tesis adlarının oluşturulması
    # -----------------------------------------------------

    unnamed_mask = (
        candidates["facility_name"]
        == ""
    )

    candidates.loc[
        unnamed_mask,
        "facility_name",
    ] = (
        "İsimsiz "
        + candidates.loc[
            unnamed_mask,
            "kategori",
        ].astype(str)
        + " - OSM "
        + candidates.loc[
            unnamed_mask,
            "id",
        ].astype(str)
    )

    candidates["facility_name_key"] = (
        candidates["facility_name"]
        .apply(normalize_text)
    )

    # -----------------------------------------------------
    # Tesis türü puanı
    # -----------------------------------------------------

    candidates["facility_type_score"] = (
        candidates["kategori"]
        .map(
            FACILITY_TYPE_SCORES
        )
        .fillna(0)
    )

    candidates["facility_access_type"] = (
        candidates.apply(
            lambda row: (
                classify_facility_access_type(
                    facility_name=row[
                        "facility_name"
                    ],
                    category=row[
                        "kategori"
                    ],
                )
            ),
            axis=1,
        )
    )

    # -----------------------------------------------------
    # Coğrafi tekrarların azaltılması
    # Yaklaşık 100 metre hassasiyetle aynı kategoriye ait
    # tekrar eden noktalar tek kayıt olarak tutulur.
    # -----------------------------------------------------

    candidates["latitude_round"] = (
        candidates["latitude"]
        .round(2)
    )

    candidates["longitude_round"] = (
        candidates["longitude"]
        .round(2)
    )

    candidates["is_named"] = (
        ~candidates["facility_name"]
        .str.startswith(
            "İsimsiz",
            na=False,
        )
    )

    candidates = (
        candidates
        .sort_values(
            by=[
                "is_named",
                "facility_type_score",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .drop_duplicates(
            subset=[
                "kategori",
                "latitude_round",
                "longitude_round",
            ],
            keep="first",
        )
        .copy()
    )

    # Aynı ilçe içinde aynı ada sahip tekrarlı kayıtlar
    # ikinci kez tekilleştirilir.
    named_candidates = candidates[
        candidates["is_named"]
    ].copy()

    unnamed_candidates = candidates[
        ~candidates["is_named"]
    ].copy()

    named_candidates = (
        named_candidates
        .drop_duplicates(
            subset=[
                "kategori",
                "province_key",
                "district_key",
                "facility_name_key",
            ],
            keep="first",
        )
    )

    candidates = pd.concat(
        [
            named_candidates,
            unnamed_candidates,
        ],
        ignore_index=True,
    )

    candidates = candidates.drop(
        columns=[
            "latitude_round",
            "longitude_round",
            "is_named",
        ],
        errors="ignore",
    )

    # -----------------------------------------------------
    # İlçe risk bilgilerinin eklenmesi
    # -----------------------------------------------------

    candidates = candidates.merge(
        risk[
            [
                "province_key",
                "district_key",
                "hazard_score",
                "final_risk_score",
            ]
        ],
        on=[
            "province_key",
            "district_key",
        ],
        how="left",
    )

    candidates["hazard_score"] = (
        candidates[
            "hazard_score"
        ].fillna(
            risk[
                "hazard_score"
            ].mean()
        )
    )

    candidates[
        "final_risk_score"
    ] = (
        candidates[
            "final_risk_score"
        ].fillna(
            risk[
                "final_risk_score"
            ].mean()
        )
    )

    return (
        candidates
        .reset_index(drop=True)
    )


# =========================================================
# LOCAL SUPPORT
# =========================================================

def calculate_local_support(
    candidates,
    osm,
):
    """
    Her aday tesisin 15 km çevresindeki
    yerel destek kapasitesini hesaplar.
    """

    support_points = osm[
        osm["kategori"].isin(
            SUPPORT_CATEGORIES
        )
    ].dropna(
        subset=[
            "latitude",
            "longitude",
        ]
    ).copy()

    support_scores = []
    hospital_counts = []
    fire_counts = []
    police_counts = []
    fuel_counts = []

    for _, facility in (
        candidates.iterrows()
    ):
        distances = haversine_distance(
            support_points["latitude"],
            support_points["longitude"],
            facility["latitude"],
            facility["longitude"],
        )

        nearby = support_points[
            distances
            <= LOCAL_SUPPORT_RADIUS_KM
        ].copy()

        category_counts = (
            nearby["kategori"]
            .value_counts()
            .to_dict()
        )

        hospital_count = (
            category_counts.get(
                "hastane",
                0,
            )
        )

        fire_count = (
            category_counts.get(
                "itfaiye",
                0,
            )
        )

        police_count = (
            category_counts.get(
                "polis",
                0,
            )
        )

        fuel_count = (
            category_counts.get(
                "akaryakit",
                0,
            )
        )

        support_score_raw = (
            hospital_count
            * SUPPORT_WEIGHTS[
                "hastane"
            ]
            + fire_count
            * SUPPORT_WEIGHTS[
                "itfaiye"
            ]
            + fuel_count
            * SUPPORT_WEIGHTS[
                "akaryakit"
            ]
            + police_count
            * SUPPORT_WEIGHTS[
                "polis"
            ]
        )

        hospital_counts.append(
            hospital_count
        )
        fire_counts.append(
            fire_count
        )
        police_counts.append(
            police_count
        )
        fuel_counts.append(
            fuel_count
        )
        support_scores.append(
            support_score_raw
        )

    result = candidates.copy()

    result[
        "nearby_hospital_count"
    ] = hospital_counts

    result[
        "nearby_fire_station_count"
    ] = fire_counts

    result[
        "nearby_police_count"
    ] = police_counts

    result[
        "nearby_fuel_station_count"
    ] = fuel_counts

    result[
        "local_support_raw"
    ] = support_scores

    result[
        "local_support_score"
    ] = min_max_scale(
        result[
            "local_support_raw"
        ]
    )

    return result


# =========================================================
# MULTIMODAL CAPABILITY
# =========================================================

def calculate_multimodal_score(
    candidates,
):
    """
    Her aday tesisin 20 km çevresindeki
    ana ulaşım türü çeşitliliğini ölçer.
    """

    multimodal_scores = []
    nearby_mode_counts = []

    maximum_mode_count = len(
        CANDIDATE_CATEGORIES
    )

    for _, facility in (
        candidates.iterrows()
    ):
        distances = haversine_distance(
            candidates["latitude"],
            candidates["longitude"],
            facility["latitude"],
            facility["longitude"],
        )

        nearby_modes = (
            candidates.loc[
                distances
                <= MULTIMODAL_RADIUS_KM,
                "kategori",
            ]
            .dropna()
            .unique()
        )

        nearby_mode_count = len(
            nearby_modes
        )

        score = (
            nearby_mode_count
            / maximum_mode_count
            * 100
        )

        nearby_mode_counts.append(
            nearby_mode_count
        )

        multimodal_scores.append(
            score
        )

    result = candidates.copy()

    result[
        "nearby_transport_mode_count"
    ] = nearby_mode_counts

    result[
        "multimodal_score"
    ] = multimodal_scores

    return result


# =========================================================
# AFFECTED AREA ACCESS
# =========================================================

def calculate_access_score(
    facilities,
    priority_districts,
):
    """
    Tesisin yüksek öncelikli ilçelere
    erişim skorunu hesaplar.
    """

    access_scores = []
    nearest_priority_distances = []
    weighted_average_distances = []

    priority_weights = (
        priority_districts[
            "dynamic_priority_score"
        ]
        .clip(lower=0)
    )

    weight_sum = (
        priority_weights.sum()
    )

    for _, facility in (
        facilities.iterrows()
    ):
        distances = haversine_distance(
            priority_districts[
                "district_latitude"
            ],
            priority_districts[
                "district_longitude"
            ],
            facility["latitude"],
            facility["longitude"],
        )

        proximity_scores = (
            100
            * np.exp(
                -distances
                / 75.0
            )
        )

        if weight_sum == 0:
            weighted_access = (
                proximity_scores.mean()
            )

            weighted_distance = (
                distances.mean()
            )
        else:
            weighted_access = (
                np.average(
                    proximity_scores,
                    weights=priority_weights,
                )
            )

            weighted_distance = (
                np.average(
                    distances,
                    weights=priority_weights,
                )
            )

        access_scores.append(
            weighted_access
        )

        nearest_priority_distances.append(
            distances.min()
        )

        weighted_average_distances.append(
            weighted_distance
        )

    result = facilities.copy()

    result[
        "affected_area_access_score"
    ] = access_scores

    result[
        "nearest_priority_district_km"
    ] = nearest_priority_distances

    result[
        "weighted_priority_distance_km"
    ] = weighted_average_distances

    return result


# =========================================================
# FACILITY SAFETY
# =========================================================

def calculate_safety_score(
    facilities,
    event_row,
):
    """
    Tesisin olay anındaki ve tarihsel
    güvenliğini hesaplar.
    """

    result = facilities.copy()

    (
        result[
            "distance_to_epicenter_km"
        ],
        result[
            "facility_event_impact_score"
        ],
    ) = calculate_facility_event_impact(
        facilities=result,
        event_latitude=event_row[
            "event_latitude"
        ],
        event_longitude=event_row[
            "event_longitude"
        ],
        magnitude=event_row[
            "magnitude"
        ],
        depth_km=event_row[
            "depth_km"
        ],
    )

    event_safety = (
        100
        - result[
            "facility_event_impact_score"
        ]
    )

    historical_hazard_safety = (
        100
        - min_max_scale(
            result[
                "hazard_score"
            ]
        )
    )

    fault_distance = (
        pd.to_numeric(
            result[
                "fay_mesafe_km"
            ],
            errors="coerce",
        )
    )

    median_fault_distance = (
        fault_distance.median()
    )

    if pd.isna(
        median_fault_distance
    ):
        median_fault_distance = 0

    fault_distance = (
        fault_distance.fillna(
            median_fault_distance
        )
    )

    fault_distance_safety = (
        min_max_scale(
            fault_distance
        )
    )

    result[
        "event_safety_score"
    ] = event_safety

    result[
        "historical_hazard_safety_score"
    ] = historical_hazard_safety

    result[
        "fault_distance_safety_score"
    ] = fault_distance_safety

    result[
        "facility_safety_score"
    ] = (
        event_safety * 0.50
        + historical_hazard_safety
        * 0.30
        + fault_distance_safety
        * 0.20
    )

    return result


# =========================================================
# EVENT-BASED RANKING
# =========================================================

def rank_facilities_for_event(
    event_id,
    event_priorities,
    candidates,
):
    """
    Tek bir deprem senaryosu için
    aday tesisleri puanlar ve sıralar.
    """

    event_df = event_priorities[
        event_priorities[
            "event_id"
        ]
        == event_id
    ].copy()

    if event_df.empty:
        raise ValueError(
            f"Event not found: {event_id}"
        )

    event_row = event_df.iloc[0]

    priority_districts = (
        event_df
        .sort_values(
            "dynamic_priority_score",
            ascending=False,
        )
        .head(10)
        .copy()
    )

    ranked = calculate_access_score(
        facilities=candidates,
        priority_districts=priority_districts,
    )

    ranked = calculate_safety_score(
        facilities=ranked,
        event_row=event_row,
    )

    ranked = ranked[
        ranked[
            "nearest_priority_district_km"
        ]
        <= MAX_PRIORITY_DISTANCE_KM
    ].copy()

    if ranked.empty:
        print(
            f"Warning: No eligible "
            f"facility for {event_id}."
        )

        return pd.DataFrame()

    ranked[
        "facility_activation_score"
    ] = (
        ranked[
            "affected_area_access_score"
        ]
        * ACTIVATION_WEIGHTS[
            "access"
        ]
        + ranked[
            "facility_safety_score"
        ]
        * ACTIVATION_WEIGHTS[
            "safety"
        ]
        + ranked[
            "facility_type_score"
        ]
        * ACTIVATION_WEIGHTS[
            "facility_type"
        ]
        + ranked[
            "multimodal_score"
        ]
        * ACTIVATION_WEIGHTS[
            "multimodal"
        ]
        + ranked[
            "local_support_score"
        ]
        * ACTIVATION_WEIGHTS[
            "local_support"
        ]
    )

    ranked["activation_rank"] = (
        ranked[
            "facility_activation_score"
        ]
        .rank(
            method="first",
            ascending=False,
        )
        .astype(int)
    )

    ranked["event_id"] = event_id

    ranked["event_name"] = (
        event_row["event_name"]
    )

    ranked["event_magnitude"] = (
        event_row["magnitude"]
    )

    ranked["event_depth_km"] = (
        event_row["depth_km"]
    )

    ranked["event_latitude"] = (
        event_row["event_latitude"]
    )

    ranked["event_longitude"] = (
        event_row["event_longitude"]
    )

    return ranked


def build_facility_rankings(
    event_priorities,
    candidates,
):
    """
    Tüm deprem senaryoları için
    tesis sıralaması üretir.
    """

    event_results = []

    event_ids = (
        event_priorities[
            "event_id"
        ]
        .drop_duplicates()
        .tolist()
    )

    for event_id in event_ids:
        event_result = (
            rank_facilities_for_event(
                event_id=event_id,
                event_priorities=event_priorities,
                candidates=candidates,
            )
        )

        if not event_result.empty:
            event_results.append(
                event_result
            )

    if not event_results:
        raise ValueError(
            "No facility ranking "
            "could be generated."
        )

    result = pd.concat(
        event_results,
        ignore_index=True,
    )

    selected_columns = [
        "event_id",
        "event_name",
        "event_magnitude",
        "event_depth_km",
        "event_latitude",
        "event_longitude",
        "id",
        "facility_name",
        "facility_name_key",
        "facility_access_type",
        "kategori",
        "alt_tur",
        "province",
        "district",
        "province_key",
        "district_key",
        "latitude",
        "longitude",
        "distance_to_epicenter_km",
        "nearest_priority_district_km",
        "weighted_priority_distance_km",
        "affected_area_access_score",
        "facility_event_impact_score",
        "event_safety_score",
        "historical_hazard_safety_score",
        "fault_distance_safety_score",
        "facility_safety_score",
        "facility_type_score",
        "nearby_transport_mode_count",
        "multimodal_score",
        "nearby_hospital_count",
        "nearby_fire_station_count",
        "nearby_police_count",
        "nearby_fuel_station_count",
        "local_support_raw",
        "local_support_score",
        "facility_activation_score",
        "activation_rank",
    ]

    return result[
        selected_columns
    ]
