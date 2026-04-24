import json
from typing import Any, Dict, List, Optional

import requests
import streamlit as st


APIFY_ACTOR_ID = "compass/crawler-google-places"
APIFY_RUN_SYNC_ENDPOINT = (
    "https://api.apify.com/v2/acts/compass~crawler-google-places/run-sync-get-dataset-items"
)


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
    """Build the exact payload sent to Apify."""
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


def run_apify_actor(api_token: str, actor_input: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run the Apify actor synchronously and return dataset items."""
    headers = {
        "Authorization": f"Bearer {api_token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    params = {
        "format": "json",
        "clean": "true",
    }

    response = requests.post(
        APIFY_RUN_SYNC_ENDPOINT,
        headers=headers,
        params=params,
        data=json.dumps(actor_input),
        timeout=330,
    )

    # Provide a useful error message if Apify rejects the request.
    if not response.ok:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise RuntimeError(f"Apify API error ({response.status_code}): {detail}")

    try:
        payload = response.json()
    except Exception as exc:
        raise RuntimeError(f"Unable to parse Apify response as JSON: {exc}")

    # The endpoint returns dataset items directly; sometimes the payload can be
    # wrapped or contain a dict depending on endpoint behavior. Normalize it.
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        # Common fallback keys, just in case Apify changes the envelope.
        for key in ("items", "data", "datasetItems", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value

    raise RuntimeError(f"Unexpected response shape from Apify: {type(payload).__name__}")


def main() -> None:
    st.set_page_config(
        page_title="Apify Google Maps Reviews Downloader",
        page_icon="⭐",
        layout="wide",
    )

    st.title("Apify Google Maps Reviews Downloader")
    st.caption(
        "Cole o link do Google Maps, ajuste os parâmetros e baixe o JSON com as reviews."
    )

    with st.sidebar:
        st.header("Conexão")
        api_token = st.text_input(
            "Apify API token",
            type="password",
            help="Cole aqui o seu token do Apify. O ideal é usar um token com permissões mínimas necessárias.",
        )
        st.markdown("---")
        st.markdown("### Dica")
        st.write(
            "O endpoint usa autenticação por Bearer token e o actor pode ser executado de forma síncrona para retornar os itens do dataset."
        )

    col1, col2 = st.columns([2, 1])

    with col1:
        place_url = st.text_input(
            "Link do Google Maps",
            value="https://maps.app.goo.gl/HAE2oiMDE4yH8Sxc8?g_st=ic",
            help="Pode ser um link curto do Google Maps, uma URL de lugar ou uma pesquisa do Maps.",
        )

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
            scrape_table_reservation_provider = st.checkbox(
                "scrapeTableReservationProvider", value=False
            )
            skip_closed_places = st.checkbox("skipClosedPlaces", value=False)
        with p3:
            verify_leads_enrichment_emails = st.checkbox(
                "verifyLeadsEnrichmentEmails", value=False
            )
            language = st.text_input("language", value="en")
            max_crawled_places_per_search = st.number_input(
                "maxCrawledPlacesPerSearch",
                min_value=1,
                max_value=1000,
                value=1,
                step=1,
            )
            max_reviews = st.number_input(
                "maxReviews",
                min_value=1,
                max_value=100000,
                value=300,
                step=10,
            )

        st.markdown("### Reviews")
        r1, r2 = st.columns(2)
        with r1:
            reviews_start_days = st.number_input(
                "reviewsStartDate (dias)",
                min_value=1,
                max_value=20000,
                value=2000,
                step=1,
                help='Será enviado ao Apify como string do tipo "2000 days".',
            )
        with r2:
            maximum_leads_enrichment_records = st.number_input(
                "maximumLeadsEnrichmentRecords",
                min_value=0,
                max_value=100000,
                value=0,
                step=1,
            )

        st.markdown("### Redes sociais")
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

    run_clicked = st.button("Rodar Apify", type="primary")

    if run_clicked:
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
                items = run_apify_actor(api_token, actor_input)
            except Exception as exc:
                st.error(str(exc))
                st.stop()

        st.success(f"Concluído. {len(items)} item(ns) retornado(s).")

        json_text = json.dumps(items, ensure_ascii=False, indent=2)
        st.download_button(
            label="Baixar JSON",
            data=json_text.encode("utf-8"),
            file_name="apify_google_maps_reviews.json",
            mime="application/json",
        )

        st.subheader("Pré-visualização")
        st.json(items[:3] if isinstance(items, list) else items)

        st.subheader("JSON bruto")
        st.code(json_text, language="json")


if __name__ == "__main__":
    main()
