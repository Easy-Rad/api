document.addEventListener('DOMContentLoaded', () => {
    // DOM element references
    const fetchButton = document.getElementById('fetchButton');
    const userSelect = document.getElementById('userSelect');
    const fromDateInput = document.getElementById('fromDate');
    const toDateInput = document.getElementById('toDate');
    const summaryContainer = document.getElementById('summaryContainer');
    const summaryContent = document.getElementById('summaryContent');
    const dataContainer = document.getElementById('dataContainer');
    const dataContent = document.getElementById('dataContent');
    const downloadContainer = document.getElementById('downloadContainer');
    const downloadJsonLink = document.getElementById('downloadJsonLink');
    const downloadCsvLink = document.getElementById('downloadCsvLink');
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
    toDateInput.value = formattedDate

    // Setup for testing
    userSelect.value = 'CARM'
    fromDateInput.value = '2025-09-20'

    /**
     * Displays a custom message box to the user.
     * @param {string} message - The message to display.
     */
    const showMessage = (message) => {
        messageText.textContent = message;
        messageBox.classList.remove('hidden');
    };

    closeMessage.addEventListener('click', () => {
        messageBox.classList.add('hidden');
    });

    // Function to convert an array of objects to a CSV string
    const convertToCSV = (data) => {
        if (!data || data.length === 0) {
            return '';
        }
        const headers = Object.keys(data[0]);
        const csvRows = [headers.join(',')]; // Add headers to the first row


        for (const row of data) {
            const values = headers.map(header => {
                const value = row[header];
                switch(header) {
                    case "exams":
                        stringValue = JSON.stringify(value)
                        break;
                    // case "report_timestamp":
                    // case "case_timestamp":
                    //     stringValue = new Date(value).toISOString()
                    //     break;
                    default:
                        stringValue = String(value)
                }
                // Handle potential commas in string values by wrapping them in quotes
                // const stringValue = (typeof value === 'object' && value !== null) ? JSON.stringify(value) : String(value);
                return `"${stringValue.replace(/"/g, '""')}"`;
            });
            csvRows.push(values.join(','));
        }
        return csvRows.join('\n');
    };

        
    const dateFormat = new Intl.DateTimeFormat("en-NZ", {
        weekday: "short",
        year: "2-digit", 
        month: "numeric",
        day: "numeric",
        hour: "numeric",
        minute: "numeric",
        second: "numeric",
        fractionalSecondDigits: 3,
        hour12: false,
    })

    const createTableCellContent = (row, header) => {
        value = row[header]
        switch(header) {
            case "exams":
                return value.map(exam => `<div>${exam}</div>`).join('')
            case "report_timestamp":
            case "case_timestamp":
                return dateFormat.format(new Date(value));
            default:
                return value;
        }
    }

    // Function to generate and display an HTML table
    const createDataTable = (data) => {
        if (!data || data.length === 0) return '';

        const headers = Object.keys(data[0]);
        let tableHTML = `
            <div class="overflow-x-auto">
                <table>
                    <thead>
                        <tr>
                            ${headers.map(header => `<th class="whitespace-nowrap">${header}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${data.map(row => `
                            <tr>
                                ${headers.map(header => `<td>${createTableCellContent(row, header)}</td>`).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;
        return tableHTML;
    };

    // Function to create and display a pivot table
    const createPivotTable = (data) => {
        if (!data || data.length === 0) return '';
        
        const pivotData = data.reduce((acc, record) => {
            const modality = record.modality;
            if (!acc[modality]) {
                acc[modality] = { count: 0, sum_of_parts: 0 };
            }
            acc[modality].count++;
            acc[modality].sum_of_parts += record.sum_of_parts;
            return acc;
        }, {});

        let totalCount = 0;
        let totalSumOfParts = 0;
        
        let pivotTableHTML = `
            <h3 class="text-lg font-semibold text-gray-700 mt-6 mb-2">Modality Summary</h3>
            <div class="overflow-x-auto">
                <table>
                    <thead>
                        <tr>
                            <th>Modality</th>
                            <th>Count</th>
                            <th>Sum of Parts</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${Object.keys(pivotData).map(modality => {
                            totalCount += pivotData[modality].count;
                            totalSumOfParts += pivotData[modality].sum_of_parts;
                            return `
                                <tr>
                                    <td>${modality}</td>
                                    <td>${pivotData[modality].count}</td>
                                    <td>${pivotData[modality].sum_of_parts}</td>
                                </tr>
                            `;
                        }).join('')}
                        <tr class="font-bold bg-gray-100">
                            <td>Total</td>
                            <td>${totalCount}</td>
                            <td>${totalSumOfParts}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        `;
        return pivotTableHTML;
    };

    // Function to generate and display all summary content
    const createResultsContent = (data) => {
        summaryContent.innerHTML = createPivotTable(data)
        dataContent.innerHTML = createDataTable(data);
    };

    fetchButton.addEventListener('click', async () => {
        const ris = userSelect.value;
        const fromDate = fromDateInput.value;
        const toDate = toDateInput.value;

        if (!ris || !fromDate || !toDate) {
            showMessage("Please select a user and a valid date range.");
            return;
        }

        // Show loading state
        buttonText.textContent = 'Fetching...';
        loadingIndicator.classList.remove('hidden');
        fetchButton.disabled = true;
        summaryContainer.classList.add('hidden');
        dataContainer.classList.add('hidden');
        downloadContainer.classList.add('hidden');

        try {
            const response = await fetch('/registrar_numbers', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ris, fromDate, toDate })
            });
            const data = await response.json();
            
            if (!data || data.length === 0) {
                showMessage("No reports found for the given user and date range.");
                return;
            }

            // Generate and display HTML content
            createResultsContent(data);
            summaryContainer.classList.remove('hidden');
            dataContainer.classList.remove('hidden');

            // Prepare data for CSV download
            const csvString = convertToCSV(data);
            const csvBlob = new Blob([csvString], { type: 'text/csv' });
            downloadCsvLink.href = URL.createObjectURL(csvBlob);

            // Prepare data for JSON download
            const jsonString = JSON.stringify(data, null, 2);
            const jsonBlob = new Blob([jsonString], { type: 'application/json' });
            downloadJsonLink.href = URL.createObjectURL(jsonBlob);
            
            downloadContainer.classList.remove('hidden');

        } catch (error) {
            console.error('Error fetching reports:', error);
            showMessage('Failed to fetch reports.');
        } finally {
            // Reset loading state
            buttonText.textContent = 'Fetch Reports';
            loadingIndicator.classList.add('hidden');
            fetchButton.disabled = false;
        }
    });
});
