# DelayPilot System Overview

## What Is the DelayPilot Data Pipeline?

The DelayPilot data pipeline is the engine behind the DelayPilot system. The web application is the part people see: dashboards, flight lists, delay predictions, and simulation screens. The pipeline is the part working behind the scenes.

Its job is to collect real-world flight and weather data, prepare that data so it is consistent and usable, run the AI prediction models, and keep the latest results ready for the web application. It runs in the background on a repeating schedule so the system stays current without manual intervention.

## What Data Does It Use?

The pipeline uses three main kinds of data.

First, it works with live and historical flight schedules and status updates for Munich Airport, covering both arrivals and departures. This information comes from an aviation data provider.

Second, it collects hourly weather information for Munich and two connected hub airports: Frankfurt and London Heathrow. This data comes from a weather data provider.

Third, it can use historical airport performance information, such as how busy the airport was and whether air traffic flow restrictions were affecting operations.

## What Does It Do With the Data?

The pipeline first cleans and organises the raw data so that different sources can be combined reliably.

It then links flight information with weather conditions to build a fuller picture of what was happening around each flight.

After that, it adds operational context. This includes how busy the airport is around the flight's scheduled time, whether the same aircraft was delayed on its previous journey, and whether that route or airline has a history of delays.

Once that combined picture is ready, the pipeline runs three AI models. These models estimate:

- the chance that a flight will be delayed by at least 15 minutes
- the chance that a flight will be delayed by at least 30 minutes
- the expected delay length in minutes

## How Does It Determine Delay Causes?

The AI models estimate delay risk and delay length, but they do not directly output a reason.

To explain the likely cause, the pipeline separately examines the underlying signals in the data. If there is rain, snow, or strong wind around Munich, weather can become the main cause. If the aircraft was already late on its previous flight, the delay is treated as a knock-on effect from earlier operations. If the airport is handling an unusually large number of flights in a short period, congestion becomes the likely explanation.

The system also looks at airline and route history when needed. These cause labels, along with percentage contributions, are then shown in the dashboard so users can understand not only the risk level but also the likely reason behind it.

## How Often Does It Update?

The pipeline refreshes automatically every 30 minutes.

On each refresh, it pulls the latest flight and weather data, rebuilds the prediction inputs, and updates the analytics that power the dashboard. This means the web application is generally showing conditions that are no more than about 30 minutes old.

## How Does It Connect to the Web Application?

The pipeline provides its own application interface for the rest of the system. The web application's backend sends requests to this interface whenever it needs flight data, predictions, weather updates, or simulation results.

The web application does not directly change the pipeline's data. It reads the latest results that the pipeline has prepared. This separation keeps the system cleaner and makes it easier to improve the pipeline without having to redesign the user interface.

## What Are the AI Models?

DelayPilot uses three AI models trained on more than 334,000 historical Munich Airport flights from February 2025 to February 2026.

These models learned patterns from real operational data, including weather, time of day, airport traffic levels, airline behaviour, and aircraft rotation history. In other words, they learned which combinations of conditions most often lead to delays.

The models were tested carefully before deployment. They are not perfect, because flight delays are influenced by many changing real-world factors, but they perform meaningfully better than chance and help identify at-risk flights earlier. That gives airport teams and decision-makers more time to prepare and respond.
