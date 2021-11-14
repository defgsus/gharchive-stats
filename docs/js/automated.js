
$(document).ready(() => {

    let repo_data = null;

    function RepoRow(data) {
        this.data = data;
        this.repo = render_cell_repo(data);
        this.repo_name = data.repo;
        this.description = data.description;
        this.push_events = data.push_events;
        this.all_push_events = data.all_push_events;
        this.commits = data.commits;
        this.distinct_commits = data.distinct_commits;
        this.push_users = data.push_users;
        this.refs = data.refs;
        this.size = data.size || "";
        this.stars = data.stars || "";
        this.stars_today = data.stars_today || "";
        this.status = data.status.startsWith("deleted")
            ? `<span class="status-deleted">${data.status}</span>`
            : data.status.startsWith("code")
                ? `<span class="status-exception">${data.status}</span>`
                : `<span class="status-active">${data.status}</span>`;
    }

    function render_name(data) {
        let markup = `<div title="${data.description ? data.description.replaceAll('\"', '&quot;') : 'no description'}">`;
        if (data.name)
            markup += `${data.name}<br>`;
        markup += `<a href="https://github.com/${data.repo}" target="_blank">${data.repo}</a>`;
        if (data.homepage)
            markup += ` (<a href="${data.homepage}" target="_blank">homepage</a>)`;
        else if (data.repo.endsWith(".github.io")) {
            const repo_name = data.repo.split('/')[1];
            markup += ` (<a href="https://${repo_name}/" target="_blank">gh-pages</a>)`;
        }
        markup += `</div>`;
        return markup;
    }

    function render_cell_repo(data) {
        let markup = render_name(data) + `<div class="timeline">`;

        const
            timeline = repo_data.timelines[data.repo],
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
    }

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
            data: data.rows.map(row => new RepoRow(row)),
            rowCallback: render_row,
            lengthMenu: [[10, 25, 50, 100, 500, -1], [10, 25, 50, 100, 500, "All"]],
            order: [[1, "asc"]],
            columns: [
                {data: "repo_name", visible: false},
                {data: "repo", title: "repo", orderData: 0},
                //{data: "description", title: "description"},
                {data: "all_push_events", title: "push events"},
                {data: "push_events", title: "distinct push events"},
                {data: "commits", title: "commits"},
                {data: "distinct_commits", title: "distinct commits"},
                {data: "push_users", title: "users"},
                {data: "refs", title: "number of refs"},
                {data: "status", title: "status"},
                {data: "size", title: "size (today)"},
                {data: "stars", title: "stars (2018)"},
                {data: "stars_today", title: "stars (today)"},
                //{data: "watchers_today", title: "watchers (today)"},

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
