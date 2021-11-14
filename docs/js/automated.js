
$(() => {

    const render_cell_repo = c => {
        return `<a href="https://github.com/${c}" target="_blank">${c}</a>`;
    };

    function start_app(data) {
        console.log($(".data-table").DataTable({
            pageLength: 25,
            data: data.rows,
            columns: [
                {data: "repo", title: "repo", render: render_cell_repo},
                {data: "push_events", title: "push events"},
                {data: "all_push_events", title: "all push events"},
                {data: "commits", title: "commits"},
                {data: "distinct_commits", title: "distinct commits"},
                {data: "push_users", title: "users"},
                {data: "refs", title: "refs"},
                {data: "stars", title: "stars"},
            ]
        }));
    }

    fetch("data/automated-2018.json")
        .then(r => r.json())
        .then(start_app)
        .catch(reason => {
            document.querySelector(".info").textContent = `failed loading data: ${reason}`;
        })


});
