import streamlit as st


# ----------------------------------------------------------
# Visual Styling
# ----------------------------------------------------------

# Adds a small amount of app-level styling while keeping Streamlit's system/browser theme behavior.
# The layout should feel more like a working dashboard without hardcoding dark or light mode.
# Returns nothing; it injects CSS into the page.

def apply_custom_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0.05rem;
            padding-bottom: 2rem;
        }

        .stAppDeployButton,
        .stDeployButton {
            display: none;
        }

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-radius: 8px;
        }

        div[data-testid="stDataFrame"] {
            border-radius: 8px;
            overflow: hidden;
        }

        .tp-access-inline {
            align-items: center;
            border: 1px solid rgba(128, 128, 128, 0.22);
            border-radius: 8px;
            display: flex;
            gap: 0.55rem;
            justify-content: flex-end;
            min-height: 2.45rem;
            padding: 0.35rem 0.55rem;
        }

        .tp-access-status {
            border-radius: 999px;
            display: inline-flex;
            font-size: 0.74rem;
            font-weight: 720;
            padding: 0.28rem 0.55rem;
            white-space: nowrap;
        }

        .tp-access-full {
            background: rgba(34, 197, 94, 0.16);
            border: 1px solid rgba(34, 197, 94, 0.34);
            color: rgb(34, 197, 94);
        }

        .tp-access-demo {
            background: rgba(59, 130, 246, 0.15);
            border: 1px solid rgba(59, 130, 246, 0.32);
            color: rgb(96, 165, 250);
        }

        .tp-access-email {
            color: rgba(128, 128, 128, 0.98);
            font-size: 0.78rem;
            line-height: 1.35;
            overflow-wrap: anywhere;
        }

        .tp-top-reference {
            align-items: center;
            color: rgba(128, 128, 128, 0.9);
            display: flex;
            font-size: 0.78rem;
            gap: 0.85rem;
            line-height: 1;
            min-height: 2.45rem;
        }

        .tp-top-reference a,
        .tp-top-reference span {
            align-items: center;
            color: inherit;
            display: inline-flex;
            gap: 0.35rem;
            text-decoration: none;
        }

        .tp-top-reference a:hover {
            color: rgba(128, 128, 128, 1);
            text-decoration: underline;
        }

        .tp-top-reference svg {
            height: 0.82rem;
            stroke: currentColor;
            stroke-width: 2;
            width: 0.82rem;
        }

        .tp-top-reference a svg {
            stroke: none;
        }

        div[data-testid="stHorizontalBlock"]:has(.tp-top-reference) {
            align-items: center;
            margin-bottom: 0.15rem;
        }

        .tp-header {
            border: 1px solid rgba(128, 128, 128, 0.22);
            border-radius: 8px;
            padding: 1.25rem 1.35rem;
            margin-bottom: 1.15rem;
        }

        .tp-header-kicker {
            color: rgba(128, 128, 128, 0.95);
            font-size: 0.86rem;
            font-weight: 600;
            margin-bottom: 0.35rem;
        }

        .tp-header-title {
            font-size: 2.25rem;
            font-weight: 760;
            letter-spacing: 0;
            line-height: 1.08;
            margin: 0;
        }

        .tp-header-subtitle {
            color: rgba(128, 128, 128, 0.98);
            font-size: 0.98rem;
            line-height: 1.45;
            margin: 0.65rem 0 0;
            max-width: 46rem;
        }

        @media (max-width: 760px) {
            .tp-header-title {
                font-size: 1.8rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
