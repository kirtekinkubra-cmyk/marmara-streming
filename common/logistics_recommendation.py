"""
common/logistics_recommendation.py

Bu modul, 10_logistics_recommendation.py dosyanizdaki ilce-tesis atama
fonksiyonlarinin degistirilmemis halidir; sadece dosya I/O ve CLI (main())
kismi kaldirildi.
"""

from pathlib import Path

import numpy as np
import pandas as pd


# Yol sabitleri kaldırıldı — girdi/çıktı Job 3 (facility_logistics_batch.py)
# tarafından, ilgili event icin filtrelenmis DataFrame olarak yonetilir.


# =========================================================
# MODEL CONFIGURATION
# =========================================================

TOP_PRIORITY_DISTRICTS = 10
TOP_FACILITIES_PER_EVENT = 3


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

    Girdiler sayısal değer veya pandas Series olabilir.
    """

    earth_radius_km = 6371.0

    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)

    delta_lat = (
        lat2_rad
        - lat1_rad
    )

    delta_lon = (
        lon2_rad
        - lon1_rad
    )

    a = (
        np.sin(
            delta_lat / 2
        ) ** 2
        + np.cos(lat1_rad)
        * np.cos(lat2_rad)
        * np.sin(
            delta_lon / 2
        ) ** 2
    )

    c = (
        2
        * np.arctan2(
            np.sqrt(a),
            np.sqrt(1 - a),
        )
    )

    return (
        earth_radius_km
        * c
    )


# =========================================================
# EVENT DATA PREPARATION
# =========================================================

def get_top_priority_districts(
    event_priorities,
    event_id,
):
    """
    Belirli bir deprem senaryosu için en yüksek
    müdahale önceliğine sahip ilçeleri seçer.
    """

    event_df = event_priorities[
        event_priorities["event_id"]
        == event_id
    ].copy()

    if event_df.empty:
        raise ValueError(
            f"No district priorities found "
            f"for event: {event_id}"
        )

    priority_districts = (
        event_df
        .sort_values(
            by=[
                "dynamic_priority_score",
                "event_impact_score",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .head(
            TOP_PRIORITY_DISTRICTS
        )
        .copy()
    )

    return priority_districts


def get_top_facilities(
    facility_scores,
    event_id,
):
    """
    Belirli bir deprem senaryosu için en yüksek
    aktivasyon skoruna sahip tesisleri seçer.
    """

    event_df = facility_scores[
        facility_scores["event_id"]
        == event_id
    ].copy()

    if event_df.empty:
        raise ValueError(
            f"No facility scores found "
            f"for event: {event_id}"
        )

    top_facilities = (
        event_df
        .sort_values(
            by=[
                "facility_activation_score",
                "affected_area_access_score",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .head(
            TOP_FACILITIES_PER_EVENT
        )
        .copy()
    )

    return top_facilities


# =========================================================
# DISTRICT-FACILITY ASSIGNMENT
# =========================================================

def assign_districts_to_facilities(
    priority_districts,
    top_facilities,
):
    """
    Her öncelikli ilçeyi aktive edilen tesisler arasından
    en yakın tesise atar.
    """

    assignments = []

    for _, district_row in (
        priority_districts.iterrows()
    ):
        facility_distances = (
            haversine_distance(
                top_facilities["latitude"],
                top_facilities["longitude"],
                district_row[
                    "district_latitude"
                ],
                district_row[
                    "district_longitude"
                ],
            )
        )

        nearest_index = (
            facility_distances.idxmin()
        )

        selected_facility = (
            top_facilities.loc[
                nearest_index
            ]
        )

        distance_km = float(
            facility_distances.loc[
                nearest_index
            ]
        )

        assignments.append(
            {
                "event_id":
                    district_row[
                        "event_id"
                    ],

                "event_name":
                    district_row[
                        "event_name"
                    ],

                "event_time":
                    district_row[
                        "event_time"
                    ],

                "event_magnitude":
                    district_row[
                        "magnitude"
                    ],

                "event_depth_km":
                    district_row[
                        "depth_km"
                    ],

                "event_latitude":
                    district_row[
                        "event_latitude"
                    ],

                "event_longitude":
                    district_row[
                        "event_longitude"
                    ],

                "province":
                    district_row[
                        "province"
                    ],

                "district":
                    district_row[
                        "district"
                    ],

                "province_key":
                    district_row[
                        "province_key"
                    ],

                "district_key":
                    district_row[
                        "district_key"
                    ],

                "district_latitude":
                    district_row[
                        "district_latitude"
                    ],

                "district_longitude":
                    district_row[
                        "district_longitude"
                    ],

                "district_priority_rank":
                    district_row[
                        "event_priority_rank"
                    ],

                "event_impact_score":
                    district_row[
                        "event_impact_score"
                    ],

                "base_risk_score":
                    district_row[
                        "base_risk_score"
                    ],

                "dynamic_priority_score":
                    district_row[
                        "dynamic_priority_score"
                    ],

                "facility_id":
                    selected_facility[
                        "id"
                    ],

                "facility_name":
                    selected_facility[
                        "facility_name"
                    ],

                "facility_category":
                    selected_facility[
                        "kategori"
                    ],

                "facility_access_type":
                    selected_facility[
                        "facility_access_type"
                    ],

                "facility_province":
                    selected_facility[
                        "province"
                    ],

                "facility_district":
                    selected_facility[
                        "district"
                    ],

                "facility_latitude":
                    selected_facility[
                        "latitude"
                    ],

                "facility_longitude":
                    selected_facility[
                        "longitude"
                    ],

                "facility_activation_rank":
                    selected_facility[
                        "activation_rank"
                    ],

                "facility_activation_score":
                    selected_facility[
                        "facility_activation_score"
                    ],

                "affected_area_access_score":
                    selected_facility[
                        "affected_area_access_score"
                    ],

                "facility_safety_score":
                    selected_facility[
                        "facility_safety_score"
                    ],

                "multimodal_score":
                    selected_facility[
                        "multimodal_score"
                    ],

                "local_support_score":
                    selected_facility[
                        "local_support_score"
                    ],

                "assignment_distance_km":
                    distance_km,
            }
        )

    result = pd.DataFrame(
        assignments
    )

    return result


# =========================================================
# RECOMMENDATION BUILDING
# =========================================================

def build_logistics_recommendations(
    event_priorities,
    facility_scores,
):
    """
    Tüm deprem senaryoları için lojistik yönlendirme
    önerilerini üretir.
    """

    event_ids = (
        event_priorities[
            "event_id"
        ]
        .drop_duplicates()
        .tolist()
    )

    all_recommendations = []

    for event_id in event_ids:
        priority_districts = (
            get_top_priority_districts(
                event_priorities=
                    event_priorities,
                event_id=event_id,
            )
        )

        top_facilities = (
            get_top_facilities(
                facility_scores=
                    facility_scores,
                event_id=event_id,
            )
        )

        event_recommendations = (
            assign_districts_to_facilities(
                priority_districts=
                    priority_districts,
                top_facilities=
                    top_facilities,
            )
        )

        all_recommendations.append(
            event_recommendations
        )

    result = pd.concat(
        all_recommendations,
        ignore_index=True,
    )

    result["assignment_rank_within_facility"] = (
        result.groupby(
            [
                "event_id",
                "facility_id",
            ]
        )[
            "dynamic_priority_score"
        ]
        .rank(
            method="first",
            ascending=False,
        )
        .astype(int)
    )

    result["assigned_district_count"] = (
        result.groupby(
            [
                "event_id",
                "facility_id",
            ]
        )[
            "district"
        ]
        .transform("count")
    )

    result["facility_total_assigned_priority"] = (
        result.groupby(
            [
                "event_id",
                "facility_id",
            ]
        )[
            "dynamic_priority_score"
        ]
        .transform("sum")
    )

    return result


# =========================================================
# KEPLER EXPORT
# =========================================================

def create_kepler_export(
    recommendations,
):
    """
    Kepler.gl üzerinde tesis-ilçe bağlantılarının
    gösterilebilmesi için uygun çıktı oluşturur.
    """

    kepler_columns = [
        "event_id",
        "event_name",
        "event_magnitude",
        "event_depth_km",
        "province",
        "district",
        "district_priority_rank",
        "dynamic_priority_score",
        "event_impact_score",
        "base_risk_score",
        "district_latitude",
        "district_longitude",
        "facility_id",
        "facility_name",
        "facility_category",
        "facility_access_type",
        "facility_province",
        "facility_district",
        "facility_activation_rank",
        "facility_activation_score",
        "facility_latitude",
        "facility_longitude",
        "assignment_distance_km",
        "assigned_district_count",
    ]

    return recommendations[
        kepler_columns
    ].copy()
