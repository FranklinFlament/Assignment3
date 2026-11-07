from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="paper_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["assignment3", "pdf-processing"],
)
def paper_pipeline():


    @task
    def scan_papers():
        import sqlite3
        from pathlib import Path

        project_dir = Path(__file__).resolve().parent.parent
        papers_dir = project_dir / "ResearchLibrary" / "papers"
        database_path = project_dir / "data" / "papers.db"

        pdf_files = sorted(papers_dir.glob("*.pdf"))

        #check if it already exists in our db
        processed_files = set()

        if database_path.exists():
            with sqlite3.connect(database_path) as conn:
                try:
                    rows = conn.execute(
                        "SELECT filename FROM papers"
                    ).fetchall()

                    processed_files = {
                        row[0] for row in rows
                    }

                except sqlite3.OperationalError as error:
                    raise RuntimeError(
                        "Could not read the papers table"
                    ) from error

        # Keep only new PDFs
        new_papers = [
            str(pdf)
            for pdf in pdf_files
            if pdf.name not in processed_files
        ]

        print(f"Total PDF files: {len(pdf_files)}", flush=True)
        print(f"Already processed: {len(pdf_files) - len(new_papers)}", flush=True)
        print(f"New PDF files: {len(new_papers)}", flush=True)

        return new_papers



    @task
    def process_papers(papers):
        import subprocess
        import sys
        import textwrap
        from pathlib import Path

        project_dir = Path(__file__).resolve().parent.parent
        markdown_dir = project_dir / "data" / "markdown"
        markdown_dir.mkdir(parents=True, exist_ok=True)

        script = textwrap.dedent("""
            import pymupdf4llm
            import sys
            from pathlib import Path

            pdf_path = sys.argv[1]
            output_path = Path(sys.argv[2])

            markdown = pymupdf4llm.to_markdown(pdf_path)
            output_path.write_text(markdown, encoding="utf-8")

            print(f"Extracted {len(markdown)} characters", flush=True)
        """)

        for paper in papers:
            pdf_path = Path(paper)
            output_path = markdown_dir / f"{pdf_path.stem}.md"

            print(f"Converting {pdf_path.name}...", flush=True)

            subprocess.run(
                [sys.executable, "-c", script, str(pdf_path), str(output_path)],
                check=True,
            )

            print(f"Saved Markdown to {output_path}", flush=True)


    @task
    def extract_metadata(papers):
        import json
        from pathlib import Path
        from openai import OpenAI

        project_dir = Path(__file__).resolve().parent.parent
        markdown_dir = project_dir / "data" / "markdown"

        client = OpenAI(
            base_url="http://localhost:11434/v1",
            api_key="ollama",
            timeout=180.0,
        )

        results = []

        for paper in papers:
            pdf_path = Path(paper)
            markdown_path = markdown_dir / f"{pdf_path.stem}.md"

            markdown = markdown_path.read_text(encoding="utf-8")

            #limit input size if too big
            content = markdown[:12000]

            prompt = f"""
Extract metadata from this scientific paper.

Return a JSON object with exactly these fields:
- title: string
- summary: string (3-5 sentences)
- keywords: list of strings (3-8 keywords)
- category: string

Use only information from the paper.
Do not invent information.

Paper content:
{content}
"""

            for attempt in range(2):
                try:
                    response = client.chat.completions.create(
                        model="llama3.2:3b",
                        messages=[
                            {
                                "role": "system",
                                "content": "Return only valid JSON."
                            },
                            {
                                "role": "user",
                                "content": prompt
                            },
                        ],
                        response_format={"type": "json_object"},
                    )

                    metadata = json.loads(
                        response.choices[0].message.content
                    )

                    required = ("title", "summary", "keywords", "category")

                    for field in required:
                        if field not in metadata:
                            raise ValueError(f"Missing field: {field}")

                    for field in ("title", "summary", "category"):
                        if not isinstance(metadata[field], str):
                            raise ValueError(f"Invalid type for {field}")

                    if (
                        not isinstance(metadata["keywords"], list)
                        or not all(
                            isinstance(k, str)
                            for k in metadata["keywords"]
                        )
                    ):
                        raise ValueError("Invalid keywords")

                    metadata["filename"] = pdf_path.name
                    results.append(metadata)

                    print(f"Extracted metadata: {pdf_path.name}", flush=True)
                    break

                except (ValueError, TypeError) as error:
                    print(
                        f"Attempt {attempt + 1} failed: {error}",
                        flush=True,
                    )

                    if attempt == 1:
                        raise

        return results
    
    
    @task
    def save_metadata(metadata_list):
        import json
        import sqlite3
        from pathlib import Path

        project_dir = Path(__file__).resolve().parent.parent
        database_path = project_dir / "data" / "papers.db"
        database_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(database_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS papers (
                    filename TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    keywords TEXT NOT NULL,
                    category TEXT NOT NULL
                )
            """)

            for metadata in metadata_list:
                conn.execute("""
                    INSERT INTO papers (
                        filename, title, summary, keywords, category
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(filename) DO UPDATE SET
                        title = excluded.title,
                        summary = excluded.summary,
                        keywords = excluded.keywords,
                        category = excluded.category
                """, (
                    metadata["filename"],
                    metadata["title"],
                    metadata["summary"],
                    json.dumps(metadata["keywords"]),
                    metadata["category"],
                ))

                print(
                    f"Saved metadata: {metadata['filename']}",
                    flush=True,
                )

        return len(metadata_list)

    
    @task
    def generate_html():
        import json
        import sqlite3
        from pathlib import Path
        from html import escape

        project_dir = Path(__file__).resolve().parent.parent
        database_path = project_dir / "data" / "papers.db"
        output_dir = project_dir / "ResearchLibrary" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(database_path) as conn:
            conn.row_factory = sqlite3.Row
            papers = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM papers ORDER BY title"
                )
            ]

        categories = sorted(set(p["category"] for p in papers))

        options = "".join(
            f'<option value="{escape(c, quote=True)}">'
            f'{escape(c)}</option>'
            for c in categories
        )

        cards = ""

        for paper in papers:
            keywords = json.loads(paper["keywords"])

            keyword_html = " ".join(
                f'<span class="keyword">{escape(k)}</span>'
                for k in keywords
            )

            cards += f"""
            <article class="paper"
                     data-category="{escape(paper['category'], quote=True)}">
                <h2>{escape(paper['title'])}</h2>
                <p class="category">{escape(paper['category'])}</p>
                <p>{escape(paper['summary'])}</p>
                <div>{keyword_html}</div>
                <p class="filename">{escape(paper['filename'])}</p>
            </article>
            """

        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport"
                  content="width=device-width, initial-scale=1">
            <title>Research Library</title>
            <style>
                body {
                    font-family: system-ui, sans-serif;
                    max-width: 1000px;
                    margin: 40px auto;
                    padding: 0 20px;
                    background: #f5f6fa;
                    color: #222;
                }
                h1 { margin-bottom: 8px; }
                .controls {
                    display: flex;
                    gap: 12px;
                    margin: 24px 0;
                }
                input, select {
                    padding: 12px;
                    border: 1px solid #ccc;
                    border-radius: 8px;
                    font-size: 15px;
                }
                input { flex: 1; }
                .paper {
                    background: white;
                    padding: 24px;
                    margin-bottom: 16px;
                    border-radius: 12px;
                    box-shadow: 0 2px 8px #00000010;
                }
                .category {
                    color: #2563eb;
                    font-weight: bold;
                }
                .keyword {
                    display: inline-block;
                    background: #e8edf5;
                    padding: 5px 10px;
                    margin: 4px;
                    border-radius: 20px;
                    font-size: 13px;
                }
                .filename {
                    color: #777;
                    font-size: 12px;
                    margin-top: 16px;
                }
            </style>
        </head>
        <body>
            <h1>Research Library</h1>
            <p>Automatically generated research paper catalog</p>

            <div class="controls">
                <input id="search" type="text"
                       placeholder="Search papers...">
                <select id="category">
                    <option value="">All categories</option>
                    __OPTIONS__
                </select>
            </div>

            <p id="count"></p>
            <div id="papers">__CARDS__</div>

            <script>
                const search = document.getElementById("search");
                const category = document.getElementById("category");
                const papers = document.querySelectorAll(".paper");
                const count = document.getElementById("count");

                function filterPapers() {
                    const query = search.value.toLowerCase();
                    const selected = category.value;
                    let visible = 0;

                    papers.forEach(paper => {
                        const matchesSearch =
                            paper.textContent.toLowerCase().includes(query);
                        const matchesCategory =
                            !selected ||
                            paper.dataset.category === selected;

                        const show = matchesSearch && matchesCategory;
                        paper.style.display = show ? "" : "none";
                        if (show) visible++;
                    });

                    count.textContent = visible + " paper(s) found";
                }

                search.addEventListener("input", filterPapers);
                category.addEventListener("change", filterPapers);
                filterPapers();
            </script>
        </body>
        </html>
        """

        html = html.replace("__OPTIONS__", options)
        html = html.replace("__CARDS__", cards)

        output_path = output_dir / "index.html"
        output_path.write_text(html, encoding="utf-8")

        print(f"Generated HTML: {output_path}", flush=True)
        print(f"Total papers: {len(papers)}", flush=True)

    
    @task
    def notify_discord(metadata_list):
        import os
        import requests

        # Do not notify if no new papers were processed
        if not metadata_list:
            print("No new papers. Skipping Discord notification.")
            return

        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")

        if not webhook_url:
            raise ValueError("DISCORD_WEBHOOK_URL is not configured")

        for metadata in metadata_list:
            title = metadata["title"]
            category = metadata["category"]
            summary = metadata["summary"]
            filename = metadata["filename"]

            message = {
                "embeds": [
                    {
                        "title": f"New research paper: {title}"[:256],
                        "description": summary[:4096],
                        "color": 3447003,
                        "fields": [
                            {
                                "name": "Category",
                                "value": category[:1024] or "Unknown",
                                "inline": True
                            },
                            {
                                "name": "File",
                                "value": filename[:1024],
                                "inline": True
                            }
                        ]
                    }
                ]
            }

            response = requests.post(
                webhook_url,
                json=message,
                timeout=15,
            )
            response.raise_for_status()

            print(f"Discord notification sent: {filename}", flush=True)


    papers = scan_papers()
    converted = process_papers(papers)
    metadata = extract_metadata(papers)
    saved = save_metadata(metadata)
    html_page = generate_html()
    notification = notify_discord(metadata)

    converted >> metadata
    saved >> html_page
    saved >> notification


paper_pipeline()