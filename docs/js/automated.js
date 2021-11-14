
$(() => {

    let repo_data = null;

    const render_cell_repo = c => {
        let markup = `<a href="https://github.com/${c}" target="_blank">${c}</a>`;
        markup += `<div class="timeline">`;

        const
            timeline = repo_data.timelines[c],
            dates = repo_data.dates;

        const max_v = timeline.reduce((s, v) => Math.max(s, v), 0);
        for (const i in timeline) {
            const
                v = timeline[i],
                d = dates[i],
                title = v
                    ? `${d}: ${v} distinct PushEvent(s)`
                    : `${d}: nothing`;

            markup += `<div class="segment" title="${title}">`;
            if (v) {
                markup += `<div class="value" style="height: ${(v/max_v*100).toFixed(2)}%;"></div>`;
            }
            markup += `</div>`;
        }
        markup += `</div>`;
        return markup;
    };

    function render_row(row, data) {
        //row.onclick = e => console.log(e);
    }

    function start_app(data) {
        data.dates = [];
        repo_data = data;

        let d = new Date(2018, 0, 1);
        while (d.getFullYear() === 2018) {
            data.dates.push(d.toDateString());
            d.setDate(d.getDate() + 1);
        }

        $(".data-table").DataTable({
            pageLength: 25,
            data: data.rows,
            rowCallback: render_row,
            columns: [
                {data: "repo", title: "repo", render: render_cell_repo},
                {data: "all_push_events", title: "push events"},
                {data: "push_events", title: "distinct push events"},
                {data: "commits", title: "commits"},
                {data: "distinct_commits", title: "distinct commits"},
                {data: "push_users", title: "users"},
                {data: "refs", title: "refs"},
                {data: "stars", title: "stars"},
            ]
        });
    }

    fetch("data/automated-2018.json")
        .then(r => r.json())
        .then(start_app)
        .catch(reason => {
            document.querySelector(".info").textContent = `failed loading data: ${reason}`;
        })

});
