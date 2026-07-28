import streamlit as st


# ----------------------------------------------------------
# Header View
# ----------------------------------------------------------

# Renders tiny app-level reference links at the top.
# The envelope and GitHub marks are inline icons so they behave like UI, not pasted text.
# Returns nothing; it writes small HTML to the page.

def render_top_reference() -> None:
    st.markdown(
        """
        <div class="tp-top-reference">
            <a href="mailto:pecek.urh@gmail.com">
                <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
                    <rect x="3" y="5" width="18" height="14" rx="2"></rect>
                    <path d="m3 7 9 6 9-6"></path>
                </svg>
                pecek.urh@gmail.com
            </a>
            <a href="https://github.com/1312Bravo/TrainingPeaks_customLog" target="_blank" rel="noopener noreferrer">
                <svg viewBox="0 0 24 24" fill="currentColor" stroke="none" aria-hidden="true">
                    <path d="M12 .5C5.65.5.5 5.65.5 12c0 5.08 3.29 9.39 7.86 10.92.58.11.79-.25.79-.56v-2.13c-3.2.7-3.87-1.36-3.87-1.36-.52-1.33-1.28-1.69-1.28-1.69-1.04-.71.08-.7.08-.7 1.15.08 1.76 1.18 1.76 1.18 1.03 1.75 2.69 1.25 3.35.95.1-.74.4-1.25.73-1.54-2.56-.29-5.25-1.28-5.25-5.7 0-1.26.45-2.29 1.18-3.1-.12-.29-.51-1.46.11-3.05 0 0 .96-.31 3.16 1.18.92-.26 1.9-.38 2.88-.39.98.01 1.96.13 2.88.39 2.2-1.49 3.16-1.18 3.16-1.18.62 1.59.23 2.76.11 3.05.73.81 1.18 1.84 1.18 3.1 0 4.43-2.7 5.4-5.26 5.69.41.36.78 1.06.78 2.13v3.17c0 .31.21.68.8.56A11.51 11.51 0 0 0 23.5 12C23.5 5.65 18.35.5 12 .5Z"></path>
                </svg>
                GitHub
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )


# Renders the top app header with owner/demo mode information.
# This replaces the large default Streamlit title plus full-width alert with a compact dashboard header.
# Returns nothing; it writes header HTML to the page.

def render_page_header(owner_mode: bool) -> None:
    subtitle = "Private training data, notes, and AI coach features are available in this workspace." if owner_mode else "Public demo experience. Private training data is hidden."

    st.markdown(
        f'''
        <section class="tp-header">
            <div class="tp-header-content">
                <div>
                    <div class="tp-header-kicker">Trail running cockpit</div>
                    <h1 class="tp-header-title">Training Platform</h1>
                    <p class="tp-header-subtitle">{subtitle}</p>
                </div>
                <svg class="tp-mountain-mark" viewBox="0 0 320 120" fill="none" aria-hidden="true">
                    <path d="M18 96h284" />
                    <path d="M28 92 82 34l34 42 24-28 44 48" />
                    <path d="M126 92 202 18l92 78" />
                    <path d="M202 18 220 58l20-18" />
                    <path d="M82 34 92 70l24-12" />
                </svg>
            </div>
        </section>
        ''',
        unsafe_allow_html=True,
    )
