import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
import streamlit as st


APIFY_RUN_SYNC_ENDPOINT = (
    "https://api.apify.com/v2/acts/compass~crawler-google-places/run-sync-get-dataset-items"
)
APIFY_RUN_ASYNC_ENDPOINT = "https://api.apify.com/v2/acts/compass~crawler-google-places/runs"
APIFY_RUN_STATUS_ENDPOINT = "https://api.apify.com/v2/actor-runs/{run_id}"
APIFY_DATASET_ITEMS_ENDPOINT = "https://api.apify.com/v2/datasets/{dataset_id}/items"

FINAL_REVIEW_FIELDS = [
    "title",
    "reviewerId",
    "reviewerUrl",
    "name",
    "reviewerNumberOfReviews",
    "isLocalGuide",
    "reviewerPhotoUrl",
    "text",
    "textTranslated",
    "publishAt",
    "publishedAtDate",
    "likesCount",
    "reviewId",
    "reviewUrl",
    "reviewOrigin",
    "stars",
    "rating",
    "responseFromOwnerDate",
    "responseFromOwnerText",
    "reviewImageUrls",
    "reviewContext",
    "reviewDetailedRating",
    "visitedIn",
    "originalLanguage",
    "translatedLanguage",
]


def build_actor_input(
    place_url: str,
    include_web_results: bool,
    language: str,
    max_crawled_places_per_search: int,
    max_reviews: int,
    maximum_leads_enrichment_records: int,
    reviews_start_days: int,
    scrape_contacts: bool,
    scrape_directories: bool,
    scrape_image_authors: bool,
    scrape_place_detail_page: bool,
    scrape_reviews_personal_data: bool,
    scrape_social_facebooks: bool,
    scrape_social_instagrams: bool,
    scrape_social_tiktoks: bool,
    scrape_social_twitters: bool,
    scrape_social_youtubes: bool,
    scrape_table_reservation_provider: bool,
    skip_closed_places: bool,
    verify_leads_enrichment_emails: bool,
) -> Dict[str, Any]:
    return {
        "includeWebResults": include_web_results,
        "language": language,
        "maxCrawledPlacesPerSearch": max_crawled_places_per_search,
        "maxReviews": max_reviews,
        "maximumLeadsEnrichmentRecords": maximum_leads_enrichment_records,
        "reviewsStartDate": f"{reviews_start_days} days",
        "scrapeContacts": scrape_contacts,
        "scrapeDirectories": scrape_directories,
        "scrapeImageAuthors": scrape_image_authors,
        "scrapePlaceDetailPage": scrape_place_detail_page,
        "scrapeReviewsPersonalData": scrape_reviews_personal_data,
        "scrapeSocialMediaProfiles": {
            "facebooks": scrape_social_facebooks,
            "instagrams": scrape_social_instagrams,
            "tiktoks": scrape_social_tiktoks,
            "twitters": scrape_social_twitters,
            "youtubes": scrape_social_youtubes,
        },
        "scrapeTableReservationProvider": scrape_table_reservation_provider,
        "skipClosedPlaces": skip_closed_places,
        "startUrls": [{"url": place_url.strip()}],
        "verifyLeadsEnrichmentEmails": verify_leads_enrichment_emails,
    }


def _extract_apify_error_detail(response: requests.Response) -> Any:
    try:
        return response.json()
    except Exception:
        return response.text


def _extract_items_from_payload(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("items", "data", "datasetItems", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value

    raise RuntimeError(f"Unexpected response shape from Apify: {type(payload).__name__}")


def _is_sync_timeout_error(response: requests.Response, detail: Any) -> bool:
    if response.status_code != 408:
        return False
    if not isinstance(detail, dict):
        return False
    error = detail.get("error")
    return isinstance(error, dict) and error.get("type") == "run-timeout-exceeded"


def _poll_apify_run_and_fetch_items(
    headers: Dict[str, str],
    actor_input: Dict[str, Any],
    poll_wait_seconds: int = 20,
    max_poll_attempts: int = 45,
) -> List[Dict[str, Any]]:
    run_response = requests.post(
        APIFY_RUN_ASYNC_ENDPOINT,
        headers=headers,
        params={"memory": 4096},
        data=json.dumps(actor_input),
        timeout=60,
    )

    if not run_response.ok:
        detail = _extract_apify_error_detail(run_response)
        raise RuntimeError(f"Apify API error ({run_response.status_code}): {detail}")

    run_payload = run_response.json()
    run_data = _as_dict(run_payload.get("data"))
    run_id = run_data.get("id")
    default_dataset_id = run_data.get("defaultDatasetId")
    if not run_id:
        raise RuntimeError("Apify não retornou o run_id ao iniciar a execução assíncrona.")

    for _ in range(max_poll_attempts):
        status_response = requests.get(
            APIFY_RUN_STATUS_ENDPOINT.format(run_id=run_id),
            headers=headers,
            params={"waitForFinish": poll_wait_seconds},
            timeout=poll_wait_seconds + 30,
        )
        if not status_response.ok:
            detail = _extract_apify_error_detail(status_response)
            raise RuntimeError(f"Apify API error ({status_response.status_code}): {detail}")

        status_payload = status_response.json()
        status_data = _as_dict(status_payload.get("data"))
        status = status_data.get("status")
        default_dataset_id = status_data.get("defaultDatasetId") or default_dataset_id

        if status == "SUCCEEDED":
            if not default_dataset_id:
                raise RuntimeError("Execução concluída, mas o dataset não foi retornado pelo Apify.")

            items_response = requests.get(
                APIFY_DATASET_ITEMS_ENDPOINT.format(dataset_id=default_dataset_id),
                headers=headers,
                params={"clean": "true", "format": "json"},
                timeout=120,
            )
            if not items_response.ok:
                detail = _extract_apify_error_detail(items_response)
                raise RuntimeError(f"Apify API error ({items_response.status_code}): {detail}")
            return _extract_items_from_payload(items_response.json())

        if status in {"FAILED", "ABORTED", "TIMED-OUT"}:
            status_message = status_data.get("statusMessage") or "sem detalhes"
            raise RuntimeError(f"Execução do actor terminou com status {status}: {status_message}")

    raise RuntimeError("Tempo limite excedido aguardando a execução assíncrona do actor no Apify.")


def run_apify_actor(api_token: str, actor_input: Dict[str, Any]) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {api_token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    params = {"format": "json", "clean": "true"}

    response = requests.post(
        APIFY_RUN_SYNC_ENDPOINT,
        headers=headers,
        params=params,
        data=json.dumps(actor_input),
        timeout=330,
    )

    if not response.ok:
        detail = _extract_apify_error_detail(response)
        if _is_sync_timeout_error(response, detail):
            return _poll_apify_run_and_fetch_items(headers=headers, actor_input=actor_input)
        raise RuntimeError(f"Apify API error ({response.status_code}): {detail}")

    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(f"Unable to parse Apify response as JSON: {exc}")

    return _extract_items_from_payload(payload)


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _clean_primitive(value: Any) -> Any:
    if isinstance(value, str):
        return value if value.strip() else None
    return value


def _normalize_review_record(raw_review: Dict[str, Any], place_title: str | None = None) -> Dict[str, Any]:
    reviewer = _as_dict(raw_review.get("reviewer"))
    owner_response = _as_dict(raw_review.get("responseFromOwner"))

    normalized = {
        "title": _clean_primitive(raw_review.get("title") or raw_review.get("placeTitle") or place_title),
        "reviewerId": _clean_primitive(raw_review.get("reviewerId") or reviewer.get("reviewerId") or reviewer.get("id")),
        "reviewerUrl": _clean_primitive(raw_review.get("reviewerUrl") or reviewer.get("reviewerUrl") or reviewer.get("url")),
        "name": _clean_primitive(raw_review.get("name") or raw_review.get("reviewerName") or reviewer.get("name")),
        "reviewerNumberOfReviews": raw_review.get("reviewerNumberOfReviews")
        if raw_review.get("reviewerNumberOfReviews") is not None
        else reviewer.get("numberOfReviews"),
        "isLocalGuide": raw_review.get("isLocalGuide")
        if raw_review.get("isLocalGuide") is not None
        else reviewer.get("isLocalGuide"),
        "reviewerPhotoUrl": _clean_primitive(raw_review.get("reviewerPhotoUrl") or reviewer.get("reviewerPhotoUrl") or reviewer.get("photoUrl")),
        "text": _clean_primitive(raw_review.get("text")),
        "textTranslated": _clean_primitive(raw_review.get("textTranslated")),
        "publishAt": _clean_primitive(raw_review.get("publishAt") or raw_review.get("publishedAt")),
        "publishedAtDate": _clean_primitive(raw_review.get("publishedAtDate") or raw_review.get("publishedAtDateTime")),
        "likesCount": raw_review.get("likesCount"),
        "reviewId": _clean_primitive(raw_review.get("reviewId") or raw_review.get("id")),
        "reviewUrl": _clean_primitive(raw_review.get("reviewUrl") or raw_review.get("url")),
        "reviewOrigin": _clean_primitive(raw_review.get("reviewOrigin") or "Google"),
        "stars": raw_review.get("stars"),
        "rating": raw_review.get("rating"),
        "responseFromOwnerDate": _clean_primitive(raw_review.get("responseFromOwnerDate") or owner_response.get("date")),
        "responseFromOwnerText": _clean_primitive(raw_review.get("responseFromOwnerText") or owner_response.get("text")),
        "reviewImageUrls": _as_list(raw_review.get("reviewImageUrls")),
        "reviewContext": _as_dict(raw_review.get("reviewContext")),
        "reviewDetailedRating": _as_dict(raw_review.get("reviewDetailedRating")),
        "visitedIn": _clean_primitive(raw_review.get("visitedIn")),
        "originalLanguage": _clean_primitive(raw_review.get("originalLanguage")),
        "translatedLanguage": _clean_primitive(raw_review.get("translatedLanguage")),
    }

    for field in FINAL_REVIEW_FIELDS:
        if field not in normalized:
            normalized[field] = None

    if normalized["reviewImageUrls"] is None:
        normalized["reviewImageUrls"] = []
    if normalized["reviewContext"] is None:
        normalized["reviewContext"] = {}
    if normalized["reviewDetailedRating"] is None:
        normalized["reviewDetailedRating"] = {}

    return {k: normalized[k] for k in FINAL_REVIEW_FIELDS}


def _extract_reviews_from_node(node: Any, inherited_title: str | None = None) -> List[Dict[str, Any]]:
    reviews: List[Dict[str, Any]] = []

    if isinstance(node, list):
        for item in node:
            reviews.extend(_extract_reviews_from_node(item, inherited_title))
        return reviews

    if not isinstance(node, dict):
        return reviews

    current_title = node.get("title") if isinstance(node.get("title"), str) else inherited_title

    nested_reviews = node.get("reviews")
    if isinstance(nested_reviews, list):
        for item in nested_reviews:
            if isinstance(item, dict):
                reviews.append(_normalize_review_record(item, place_title=current_title))

    has_review_shape = any(
        key in node
        for key in (
            "reviewId",
            "reviewUrl",
            "reviewerId",
            "reviewerUrl",
            "stars",
            "text",
            "publishedAtDate",
        )
    )
    if has_review_shape:
        reviews.append(_normalize_review_record(node, place_title=inherited_title))

    for value in node.values():
        if isinstance(value, (list, dict)):
            reviews.extend(_extract_reviews_from_node(value, current_title))

    return reviews


def normalize_reviews_to_apify_format(raw_payload: Any) -> List[Dict[str, Any]]:
    flat_reviews = _extract_reviews_from_node(raw_payload)

    deduped: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    for review in flat_reviews:
        dedupe_key = review.get("reviewId") or review.get("reviewUrl") or json.dumps(review, sort_keys=True, ensure_ascii=False)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(review)

    return deduped


def _slugify_place_name(place_name: str) -> str:
    plain = unicodedata.normalize("NFKD", place_name).encode("ascii", "ignore").decode("ascii")
    plain = plain.lower().strip()
    plain = re.sub(r"[^a-z0-9\s_-]", "", plain)
    plain = re.sub(r"[\s-]+", "_", plain)
    plain = plain.strip("_")
    return plain or "local"


def generate_export_filename(place_name: str, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    slug_name = _slugify_place_name(place_name)
    return f"{slug_name}_reviews_{now.strftime('%m_%y')}.json"


def main() -> None:
    st.set_page_config(page_title="Apify Google Maps Reviews Downloader", page_icon="⭐", layout="wide")

    st.title("Apify Google Maps Reviews Downloader")
    st.caption("Cole o link do Google Maps, normalize e baixe o JSON final no formato flat de reviews (estilo export Apify).")

    with st.sidebar:
        st.header("Conexão")
        api_token = st.text_input("Apify API token", type="password")

    col1, col2 = st.columns([2, 1])

    with col1:
        place_url = st.text_input("Link do Google Maps", value="https://maps.app.goo.gl/HAE2oiMDE4yH8Sxc8?g_st=ic")

        st.subheader("Parâmetros")
        p1, p2, p3 = st.columns(3)
        with p1:
            include_web_results = st.checkbox("includeWebResults", value=False)
            scrape_contacts = st.checkbox("scrapeContacts", value=False)
            scrape_directories = st.checkbox("scrapeDirectories", value=False)
            scrape_image_authors = st.checkbox("scrapeImageAuthors", value=False)
        with p2:
            scrape_place_detail_page = st.checkbox("scrapePlaceDetailPage", value=False)
            scrape_reviews_personal_data = st.checkbox("scrapeReviewsPersonalData", value=True)
            scrape_table_reservation_provider = st.checkbox("scrapeTableReservationProvider", value=False)
            skip_closed_places = st.checkbox("skipClosedPlaces", value=False)
        with p3:
            verify_leads_enrichment_emails = st.checkbox("verifyLeadsEnrichmentEmails", value=False)
            language = st.text_input("language", value="en")
            max_crawled_places_per_search = st.number_input("maxCrawledPlacesPerSearch", min_value=1, max_value=1000, value=1, step=1)
            max_reviews = st.number_input("maxReviews", min_value=1, max_value=100000, value=300, step=10)

        r1, r2 = st.columns(2)
        with r1:
            reviews_start_days = st.number_input("reviewsStartDate (dias)", min_value=1, max_value=20000, value=2000, step=1)
        with r2:
            maximum_leads_enrichment_records = st.number_input("maximumLeadsEnrichmentRecords", min_value=0, max_value=100000, value=0, step=1)

        s1, s2, s3 = st.columns(3)
        with s1:
            scrape_social_facebooks = st.checkbox("facebooks", value=False)
            scrape_social_instagrams = st.checkbox("instagrams", value=False)
        with s2:
            scrape_social_tiktoks = st.checkbox("tiktoks", value=False)
            scrape_social_twitters = st.checkbox("twitters", value=False)
        with s3:
            scrape_social_youtubes = st.checkbox("youtubes", value=False)

    with col2:
        st.subheader("Payload")
        preview_payload = build_actor_input(
            place_url=place_url,
            include_web_results=include_web_results,
            language=language,
            max_crawled_places_per_search=max_crawled_places_per_search,
            max_reviews=max_reviews,
            maximum_leads_enrichment_records=maximum_leads_enrichment_records,
            reviews_start_days=reviews_start_days,
            scrape_contacts=scrape_contacts,
            scrape_directories=scrape_directories,
            scrape_image_authors=scrape_image_authors,
            scrape_place_detail_page=scrape_place_detail_page,
            scrape_reviews_personal_data=scrape_reviews_personal_data,
            scrape_social_facebooks=scrape_social_facebooks,
            scrape_social_instagrams=scrape_social_instagrams,
            scrape_social_tiktoks=scrape_social_tiktoks,
            scrape_social_twitters=scrape_social_twitters,
            scrape_social_youtubes=scrape_social_youtubes,
            scrape_table_reservation_provider=scrape_table_reservation_provider,
            skip_closed_places=skip_closed_places,
            verify_leads_enrichment_emails=verify_leads_enrichment_emails,
        )
        st.code(json.dumps(preview_payload, ensure_ascii=False, indent=2), language="json")

    if st.button("Rodar Apify", type="primary"):
        if not api_token.strip():
            st.error("Informe o token do Apify na barra lateral.")
            st.stop()
        if not place_url.strip():
            st.error("Informe um link válido do Google Maps.")
            st.stop()

        actor_input = build_actor_input(
            place_url=place_url,
            include_web_results=include_web_results,
            language=language,
            max_crawled_places_per_search=max_crawled_places_per_search,
            max_reviews=max_reviews,
            maximum_leads_enrichment_records=maximum_leads_enrichment_records,
            reviews_start_days=reviews_start_days,
            scrape_contacts=scrape_contacts,
            scrape_directories=scrape_directories,
            scrape_image_authors=scrape_image_authors,
            scrape_place_detail_page=scrape_place_detail_page,
            scrape_reviews_personal_data=scrape_reviews_personal_data,
            scrape_social_facebooks=scrape_social_facebooks,
            scrape_social_instagrams=scrape_social_instagrams,
            scrape_social_tiktoks=scrape_social_tiktoks,
            scrape_social_twitters=scrape_social_twitters,
            scrape_social_youtubes=scrape_social_youtubes,
            scrape_table_reservation_provider=scrape_table_reservation_provider,
            skip_closed_places=skip_closed_places,
            verify_leads_enrichment_emails=verify_leads_enrichment_emails,
        )

        with st.spinner("Executando o actor no Apify..."):
            try:
                raw_items = run_apify_actor(api_token, actor_input)
                normalized_reviews = normalize_reviews_to_apify_format(raw_items)
            except Exception as exc:
                st.error(str(exc))
                st.stop()

        place_name = (normalized_reviews[0].get("title") if normalized_reviews else None) or "local"
        export_filename = generate_export_filename(place_name)
        json_text = json.dumps(normalized_reviews, ensure_ascii=False, indent=2)

        st.success(f"Concluído. {len(normalized_reviews)} review(s) normalizada(s) em lista flat.")

        st.download_button(
            label="Baixar JSON final (flat)",
            data=json_text.encode("utf-8"),
            file_name=export_filename,
            mime="application/json",
        )

        st.subheader("Preview do JSON final")
        st.json(normalized_reviews[:3])

        st.subheader("JSON final")
        st.code(json_text, language="json")


if __name__ == "__main__":
    main()
