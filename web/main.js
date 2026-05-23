document.addEventListener("DOMContentLoaded", function () {

    const priceTable = document.getElementById("priceTable");

    const eventSource = new EventSource("/stream");

    const prices = {};

    eventSource.onmessage = function (event) {

        const processedData = JSON.parse(event.data);

        console.log(processedData);

        // use lowercase field names
        prices[processedData.currency] = processedData;

        updatePriceTable();
    };

    eventSource.onerror = function (error) {
        console.error("SSE error:", error);
    };

    function updatePriceTable() {

        while (priceTable.rows.length > 1) {
            priceTable.deleteRow(1);
        }

        Object.values(prices).forEach((item) => {

            const row = priceTable.insertRow(-1);

            const currencyCell = row.insertCell(0);
            const priceCell = row.insertCell(1);
            const timeCell = row.insertCell(2);

            currencyCell.textContent = item.currency;
            priceCell.textContent = Number(item.price).toFixed(2);
            timeCell.textContent = item.time;
        });
    }
});