import 'flowbite';
import { DataTable } from 'simple-datatables';
import ApexCharts from 'apexcharts';

const createPivotTable = data =>
    `<thead class="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
        <tr>
            <th scope="col" class="px-6 py-3">Modality</th>
            <th scope="col" class="px-6 py-3">Exam Count</th>
            <th scope="col" class="px-6 py-3">Exam Parts</th>
        </tr>
    </thead>
    <tbody>${ data.index.map((modality, row_index) => 
        '<tr class="bg-white border-b dark:bg-gray-800 dark:border-gray-700 border-gray-200">' +
        `<th scope="row" class="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">${modality}</th>` +    
        data.data[row_index].map(td => `<td class="px-6 py-4">${td}</td>`).join('') +
        '</tr>').join('')
    }</tbody>
    <tfoot>
        <tr class="font-semibold text-gray-900 dark:text-white">
            <th scope="row" class="px-6 py-3 text-base">All</th>
            ${data.columns.map((_j, i) => `<td class="px-6 py-3">${
                data.data
                    .map(row => row[i])
                    .reduce((accumulator, currentValue) => accumulator + currentValue, 0)
                }</td>`).join('')
            }
        </tr>
    </tfoot>`

const dateFormat = new Intl.DateTimeFormat("en-NZ", {
    weekday: "short",
    year: "2-digit", 
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
});

const dataTableConfig = {
    columns: [
        { select: [0, 6], type: "number", render: value => dateFormat.format(value*1000)},
        { select: [1, 3], type: "string", sortable: false, searchable: false},
        { select: [2, 5], type: "string"},
        { select: 4, type: "string", render: value => value.join("<br>")},
        { select: [7, 8], type: "number", searchable: false},
    ],
    paging: true,
    perPage: 10,
    perPageSelect: [10, 20, 50, 100],
    data: {
        headings: ['Report timestamp', 'Action', 'Accession', 'Modality', 'Exams', 'Description', 'Case timestamp', 'Age', 'Exam parts'],
        data: [],
    },
    template: (options, dom) => "<div class='" + options.classes.top + "'>" +
            "<div class='flex flex-col sm:flex-row sm:items-center space-y-4 sm:space-y-0 sm:space-x-3 rtl:space-x-reverse w-full sm:w-auto'>" +
            (options.paging && options.perPageSelect ?
                "<div class='" + options.classes.dropdown + "'>" +
                    "<label>" +
                        "<select class='" + options.classes.selector + "'></select> " + options.labels.perPage +
                    "</label>" +
                "</div>" : ""
            ) + "<button id='exportDropdownButton' type='button' class='flex w-full items-center justify-center rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-900 hover:bg-gray-100 hover:text-primary-700 focus:z-10 focus:outline-none focus:ring-4 focus:ring-gray-100 dark:border-gray-600 dark:bg-gray-800 dark:text-gray-400 dark:hover:bg-gray-700 dark:hover:text-white dark:focus:ring-gray-700 sm:w-auto'>" +
            "Export as" +
            "<svg class='-me-0.5 ms-1.5 h-4 w-4' aria-hidden='true' xmlns='http://www.w3.org/2000/svg' width='24' height='24' fill='none' viewBox='0 0 24 24'>" +
                "<path stroke='currentColor' stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='m19 9-7 7-7-7' />" +
            "</svg>" +
        "</button>" +
        "<div id='exportDropdown' class='z-10 hidden w-52 divide-y divide-gray-100 rounded-lg bg-white shadow-sm dark:bg-gray-700' data-popper-placement='bottom'>" +
            "<ul class='p-2 text-left text-sm font-medium text-gray-500 dark:text-gray-400' aria-labelledby='exportDropdownButton'>" +
                "<li>" +
                    "<button id='export-csv' class='group inline-flex w-full items-center rounded-md px-3 py-2 text-sm text-gray-500 hover:bg-gray-100 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-white'>" +
                        "<svg class='me-1.5 h-4 w-4 text-gray-400 group-hover:text-gray-900 dark:text-gray-400 dark:group-hover:text-white' aria-hidden='true' xmlns='http://www.w3.org/2000/svg' width='24' height='24' fill='currentColor' viewBox='0 0 24 24'>" +
                            "<path fill-rule='evenodd' d='M9 2.221V7H4.221a2 2 0 0 1 .365-.5L8.5 2.586A2 2 0 0 1 9 2.22ZM11 2v5a2 2 0 0 1-2 2H4a2 2 0 0 0-2 2v7a2 2 0 0 0 2 2 2 2 0 0 0 2 2h12a2 2 0 0 0 2-2 2 2 0 0 0 2-2v-7a2 2 0 0 0-2-2V4a2 2 0 0 0-2-2h-7Zm1.018 8.828a2.34 2.34 0 0 0-2.373 2.13v.008a2.32 2.32 0 0 0 2.06 2.497l.535.059a.993.993 0 0 0 .136.006.272.272 0 0 1 .263.367l-.008.02a.377.377 0 0 1-.018.044.49.49 0 0 1-.078.02 1.689 1.689 0 0 1-.297.021h-1.13a1 1 0 1 0 0 2h1.13c.417 0 .892-.05 1.324-.279.47-.248.78-.648.953-1.134a2.272 2.272 0 0 0-2.115-3.06l-.478-.052a.32.32 0 0 1-.285-.341.34.34 0 0 1 .344-.306l.94.02a1 1 0 1 0 .043-2l-.943-.02h-.003Zm7.933 1.482a1 1 0 1 0-1.902-.62l-.57 1.747-.522-1.726a1 1 0 0 0-1.914.578l1.443 4.773a1 1 0 0 0 1.908.021l1.557-4.773Zm-13.762.88a.647.647 0 0 1 .458-.19h1.018a1 1 0 1 0 0-2H6.647A2.647 2.647 0 0 0 4 13.647v1.706A2.647 2.647 0 0 0 6.647 18h1.018a1 1 0 1 0 0-2H6.647A.647.647 0 0 1 6 15.353v-1.706c0-.172.068-.336.19-.457Z' clip-rule='evenodd'/>" +
                        "</svg>" +
                        "<a id='downloadCsvLink'>Export CSV</a>" +
                    "</button>" +
                "</li>" +
                "<li>" +
                    "<button id='export-json' class='group inline-flex w-full items-center rounded-md px-3 py-2 text-sm text-gray-500 hover:bg-gray-100 hover:text-gray-900 dark:text-gray-400 dark:hover:bg-gray-600 dark:hover:text-white'>" +
                        "<svg class='me-1.5 h-4 w-4 text-gray-400 group-hover:text-gray-900 dark:text-gray-400 dark:group-hover:text-white' aria-hidden='true' xmlns='http://www.w3.org/2000/svg' width='24' height='24' fill='currentColor' viewBox='0 0 24 24'>" +
                            "<path fill-rule='evenodd' d='M9 2.221V7H4.221a2 2 0 0 1 .365-.5L8.5 2.586A2 2 0 0 1 9 2.22ZM11 2v5a2 2 0 0 1-2 2H4v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2h-7Zm-.293 9.293a1 1 0 0 1 0 1.414L9.414 14l1.293 1.293a1 1 0 0 1-1.414 1.414l-2-2a1 1 0 0 1 0-1.414l2-2a1 1 0 0 1 1.414 0Zm2.586 1.414a1 1 0 0 1 1.414-1.414l2 2a1 1 0 0 1 0 1.414l-2 2a1 1 0 0 1-1.414-1.414L14.586 14l-1.293-1.293Z' clip-rule='evenodd'/>" +
                        "</svg>" +
                        "<a id='downloadJsonLink'>Export JSON</a>" +
                    "</button>" +
                "</li>" +
            "</ul>" +
        "</div>" + "</div>" +
            (options.searchable ?
                "<div class='" + options.classes.search + "'>" +
                    "<input class='" + options.classes.input + "' placeholder='" + options.labels.placeholder + "' type='search' title='" + options.labels.searchTitle + "'" + (dom.id ? " aria-controls='" + dom.id + "'" : "") + ">" +
                "</div>" : ""
            ) +
        "</div>" +
        "<div class='" + options.classes.container + "'" + (options.scrollY.length ? " style='height: " + options.scrollY + "; overflow-Y: auto;'" : "") + "></div>" +
        "<div class='" + options.classes.bottom + "'>" +
            (options.paging ?
                "<div class='" + options.classes.info + "'></div>" : ""
            ) +
            "<nav class='" + options.classes.pagination + "'></nav>" +
        "</div>",
};

const chartColours = {CT:'#008FFB', MR:'#00E396', NM:'#FEB019', XR: '#FF4560'};

const chartConfig = {
    chart: {
        type: 'line',
        zoom: {
            type: 'xy',
            autoScaleYaxis: true
        },
    },
    tooltip: {
        followCursor: true,
        shared: true,
        x: {
            show: true,
        }
    },
    annotations: {
        yaxis: [
            {
                y: 10000,
                borderColor: chartColours.XR,
                strokeDashArray: 0,
                label: {
                    borderColor: chartColours.XR,
                    text: 'General XR target',
                },
            },
            {
                y: 5000,
                borderColor: chartColours.CT,
                strokeDashArray: 0,
                label: {
                    borderColor: chartColours.CT,
                    text: 'CT target',
                },
            },
            {
                y: 750,
                borderColor: chartColours.MR,
                strokeDashArray: 0,
                label: {
                    borderColor: chartColours.MR,
                    text: 'MR target',
                },
            },
            {
                y: 200,
                borderColor: chartColours.NM,
                strokeDashArray: 0,
                label: {
                    borderColor: chartColours.NM,
                    text: 'NM target',
                },
            },
        ]
    },
    series: [],
    yaxis: {
        min: 0,
        decimalsInFloat: 0,
        forceNiceScale: true,
    },
    xaxis: {
        type: 'datetime',
        tooltip: {
            enabled: false,
        },
        labels: {
            datetimeUTC: false,
        }
    },
    legend: {
        position: 'top',
    },
}

const convertToCSV = (data) => {
    if (!data || data.length === 0) {
        return '';
    }
    const headers = dataTableConfig.data.headings;
    const csvRows = [headers.join(',')];

    for (const row of data) {
        const values = headers.map((header, index) => {
            const value = row[index];
            let stringValue;
            switch(header) {
                case "Exams":
                    stringValue = JSON.stringify(value)
                    break;
                case "Report timestamp":
                case "Case timestamp":
                    stringValue = new Date(value*1000).toISOString()
                    break;
                default:
                    stringValue = String(value)
            }
            // Handle potential commas in string values by wrapping them in quotes
            return `"${stringValue.replace(/"/g, '""')}"`;
        });
        csvRows.push(values.join(','));
    }
    return csvRows.join('\n');
};


document.addEventListener('DOMContentLoaded', () => {
    // DOM element references
    const fetchButton = document.getElementById('fetchButton');
    const userSelect = document.getElementById('userSelect');
    const fromDateInput = document.getElementById('fromDate');
    const toDateInput = document.getElementById('toDate');
    const summaryTable = document.getElementById('summaryTable');
    const dataContainer = document.getElementById('dataContainer');
    const progressContainer = document.getElementById('progressContainer');
    const progressMsg = document.getElementById('progressMsg');
    const progressBar = document.getElementById('progressBar');
    const loadingIndicator = document.getElementById('loadingIndicator');
    const buttonText = document.getElementById('buttonText');
    const messageBox = document.getElementById('messageBox');
    const messageText = document.getElementById('messageText');
    const closeMessage = document.getElementById('closeMessage');

    const today = new Date();
    // Format the date to 'YYYY-MM-DD' which is the required format for date input type
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
    const day = String(today.getDate()).padStart(2, '0');
    const formattedDate = `${year}-${month}-${day}`;
    fromDateInput.value = toDateInput.value = formattedDate

    userSelect.onchange = () => {
        fromDateInput.value = userSelect.options[userSelect.selectedIndex].dataset.start;
    }

    const showMessage = (message) => {
        messageText.textContent = message;
        messageBox.classList.remove('hidden');
    };

    closeMessage.addEventListener('click', () => {
        messageBox.classList.add('hidden');
    });

    const dataTable = new DataTable("#dataContent", dataTableConfig);
    const $exportButton = document.getElementById("exportDropdownButton");
    const $exportDropdownEl = document.getElementById("exportDropdown");
    const dropdown = new Dropdown($exportDropdownEl, $exportButton);

    fetchButton.addEventListener('click', async () => {

        const ris = userSelect.value;
        const fromDate = fromDateInput.value;
        const toDate = toDateInput.value;

        if (!ris || !fromDate || !toDate) {
            showMessage("Please select a user and a valid date range.");
            return;
        }

        fetchButton.disabled = true;
        buttonText.textContent = 'Fetching...';
        loadingIndicator.classList.remove('invisible');

        progressMsg.textContent = '';
        progressBar.style.width = '0px';
        progressContainer.classList.remove('hidden');

        dataContainer.classList.add('hidden');
        
        const webSocketEndpoint = '/registrar_numbers'
        let websocket;
        try {
            websocket = new WebSocket(webSocketEndpoint);
        } catch {
            const webSocketScheme = window.location.protocol === "https:" ? "wss:" : "ws:";
            const webSocketURL = webSocketScheme + "//" + window.location.host + webSocketEndpoint;
            websocket = new WebSocket(webSocketURL);
        }
        websocket.addEventListener("open", () => {
            console.log('websocket opened');
            websocket.send(JSON.stringify({ ris, fromDate, toDate }));
        });
        websocket.addEventListener("close", () => {
            console.log('websocket closed');
            loadingIndicator.classList.add('invisible');
            buttonText.textContent = 'Fetch Reports';
            fetchButton.disabled = false;
        });
        websocket.addEventListener("message", (e) => {
            const message = JSON.parse(e.data);
            switch (message.type) {
                case "update":
                    console.log(`websocket update (${message.percent}%): ${message.msg}`);
                    progressMsg.textContent = message.msg;
                    progressBar.style.width = `${message.percent}%`;
                    break;
                case "error":
                    console.error(`websocket error: ${message.msg}`);
                    progressContainer.classList.add('hidden');
                    showMessage(message.msg);
                    break;
                case "result":
                    console.log('websocket got result');
                    progressMsg.textContent = 'Completed';
                    progressBar.style.width = '100%';
                    const data = message.result;
                    if (data === null) {
                        showMessage("No reports found for the given user and date range.");
                    } else {
                        const reportData = data['report_data']
                        dataTable.data.data = [];
                        dataTable.insert({data: reportData});
                        summaryTable.innerHTML = createPivotTable(data['modality_pivot']);

                        const chart = new ApexCharts(document.getElementById('chart'), chartConfig);
                        chart.render();
                        chart.updateOptions({
                            colors: data.chart_data.map(s => chartColours[s.name]),
                            series: data.chart_data,
                        });

                        const csvString = convertToCSV(reportData);
                        const csvBlob = new Blob([csvString], { type: 'text/csv' });
                        const downloadCsvLink = document.getElementById('downloadCsvLink');
                        downloadCsvLink.download = `${ris}_${fromDateInput.value}_${toDateInput.value}.csv`
                        downloadCsvLink.href = URL.createObjectURL(csvBlob);

                        const jsonString = JSON.stringify(reportData, null, 2);
                        const jsonBlob = new Blob([jsonString], { type: 'application/json' });
                        const downloadJsonLink = document.getElementById('downloadJsonLink');
                        downloadJsonLink.download = `${ris}_${fromDateInput.value}_${toDateInput.value}.json`
                        downloadJsonLink.href = URL.createObjectURL(jsonBlob);
                        dataContainer.classList.remove('hidden');
                    }
                    progressContainer.classList.add('hidden');
                    break;
                }
        });

    });
});
