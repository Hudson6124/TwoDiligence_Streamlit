import streamlit as st
import mysql.connector
import pandas as pd
import time
from queue import Queue

def main_page():
    from queue import Queue
    import time
    import streamlit as st

    # Definition of stop_executing function before it's used
    def stop_executing():
        nonlocal task_in_progress
        task_in_progress = False
        query_execution_container.empty()
        execute_button.button("Execute Queries", key="execute_button_after_stop")

    cities = [
        "New York, NY", "Los Angeles, CA", "Chicago, IL", "Philadelphia, PA",
        "Dallas-Fort Worth, TX", "San Francisco-Oakland-San Jose, CA",
        "Washington, DC", "Houston, TX", "Boston, MA", "Atlanta, GA"
    ]
    industries = [
        "Restaurants", "Bars", "Plumbing", "Electrical", "Healthcare",
        "Legal Services", "Retail", "Automotive Services", "IT Services", "Real Estate"
    ]
    business_pages = list(range(1, 11))
    review_pages = list(range(1, 11))

    st.title("Data Scraper and Integrator Dashboard")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.header("Query Parameters")
        selected_city = st.selectbox("Select Area", cities)
        selected_industries = st.multiselect("Select Industries", industries + ["Select All Industries"])
        selected_business_pages = st.selectbox("Select Number of Business Pages", [f"{i} pages" for i in business_pages])
        selected_review_pages = st.selectbox("Select Number of Review Pages", [f"{i} pages" for i in review_pages])

        if st.button("Add to Query List", key="add_query_button"):
            if not selected_city or not selected_industries or not selected_business_pages or not selected_review_pages:
                st.warning("Please select all parameters before adding to the query list.")
            else:
                if "all" in selected_industries:
                    selected_industries = industries[:]
                new_queries = [
                    f"{selected_city} - {industry} - {selected_business_pages} - {selected_review_pages}"
                    for industry in selected_industries
                ]
                st.session_state.queries = st.session_state.get("queries", []) + new_queries

    with col2:
        st.header("Query List")
        query_list_container = st.container()
        with query_list_container:
            if "queries" not in st.session_state:
                st.write("No queries added yet.")
            else:
                if len(st.session_state.queries) != len(set(st.session_state.queries)):
                    st.warning("You have duplicate queries.")
                for i, query in enumerate(st.session_state.queries):
                    cols = st.columns([9, 1])
                    cols[0].write(f"- {query}")
                    if cols[1].button("X", key=f"remove_{i}"):
                        st.session_state.queries.pop(i)
                        st.experimental_rerun()

    with col3:
        st.header("Query Execution")
        query_execution_container = st.container()

        execute_button = st.empty()
        stop_button = st.empty()

        if execute_button.button("Execute Queries", key="execute_button"):
            if "queries" not in st.session_state or len(st.session_state.queries) == 0:
                st.warning("Please add at least one query to the query list before executing.")
            else:
                task_queue = Queue()
                task_in_progress = True

                execute_button.empty()
                stop_button.button("Stop Executing", on_click=stop_executing, key="stop_button")

                with query_execution_container:
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    for query in st.session_state.queries:
                        parts = query.split(' - ')
                        city, industry, business_pages, review_pages = parts

                        status_text.text(f"Starting scraping task for {city} - {industry}...")
                        progress_bar.progress(0)

                        time.sleep(1)
                        task_data = f"Scraping Google for {city} - {industry}..."
                        task_queue.put(task_data)
                        status_text.text(task_data)
                        progress_bar.progress(20)

                        time.sleep(1)
                        task_queue.put(f"Scraping Google for {city} - {industry} ✔️")
                        status_text.text(f"Scraping Google for {city} - {industry} ✔️")
                        progress_bar.progress(40)

                        time.sleep(1)
                        task_queue.put(f"Scraping Yelp for {city} - {industry}...")
                        status_text.text(f"Scraping Yelp for {city} - {industry}...")
                        progress_bar.progress(60)

                        time.sleep(1)
                        task_queue.put(f"Scraping Yelp for {city} - {industry} ✔️")
                        status_text.text(f"Scraping Yelp for {city} - {industry} ✔️")
                        progress_bar.progress(80)

                        time.sleep(1)
                        task_queue.put(f"Running analysis for {city} - {industry}...")
                        status_text.text(f"Running analysis for {city} - {industry}...")
                        progress_bar.progress(90)

                        time.sleep(1)
                        task_queue.put(f"Data analysis for {city} - {industry} ✔️")
                        status_text.text(f"Data analysis for {city} - {industry} ✔️")
                        progress_bar.progress(100)

                        task_queue.put(f"{city} - {industry}: Complete ✔️")
                        status_text.text(f"{city} - {industry}: Complete ✔️")

                        if not task_in_progress:
                            break

                    task_in_progress = False
                    stop_button.empty()
                    execute_button.button("Execute Queries")
                    status_text.text("All queries executed successfully!")

def load_data():
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Htl77856!!!",
            database="TwoDiligence_April"
        )
        cursor = db.cursor()
        cursor.execute("SELECT name, industry, area, average_rating, average_5_point_sentiment, twodiligence_score FROM ZZZZ")
        data = cursor.fetchall()

        # Get the column names from the table
        column_names = [i[0] for i in cursor.description]

        cursor.close()
        db.close()

        df = pd.DataFrame(data, columns=column_names)

        # Rename the columns
        df.columns = ["Name", "Industry", "Area", "Average Rating", "Average 5 Point Sentiment", "Two Diligence Score"]

        # Add a "Rank" column based on the "Two Diligence Score"
        df["Rank"] = df["Two Diligence Score"].rank(ascending=False).astype(int)

        # Sort DataFrame by "Rank"
        df = df.sort_values("Rank")

        # Reorder the columns for better presentation
        df = df[["Rank", "Name", "Industry", "Area", "Average Rating", "Average 5 Point Sentiment", "Two Diligence Score"]]

        return df

    except Exception as e:
        st.error(f"Failed to retrieve results. Error: {str(e)}")
        return None

def results_page():
    st.title("Business Analysis Results")

    df = load_data()

    if df is not None:
        # Get unique industries and areas for filtering
        industries = df["Industry"].unique()
        areas = df["Area"].unique()

        # Create filter options
        selected_industries = st.multiselect("Select Industries", industries)
        selected_areas = st.multiselect("Select Areas", areas)

        # Apply filters
        if selected_industries:
            df = df[df["Industry"].isin(selected_industries)]
        if selected_areas:
            df = df[df["Area"].isin(selected_areas)]

        # Update the "Rank" column based on the filtered data
        df["Rank"] = df["Two Diligence Score"].rank(ascending=False).astype(int)

        # Display the DataFrame
        st.dataframe(df.set_index("Rank"))

        # Add a download button
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name="business_analysis_results.csv",
            mime="text/csv"
        )

def main():
    st.set_page_config(page_title="TwoDiligence", page_icon=":bar_chart:", layout="wide")
    st.sidebar.image("two_dil_logo.png", use_column_width=True)

    menu = ["Main Page", "Results Page"]
    choice = st.sidebar.radio("Select Page", menu)
    if choice == "Main Page":
        main_page()
    elif choice == "Results Page":
        st.sidebar.empty()
        results_page()

if __name__ == "__main__":
    main()