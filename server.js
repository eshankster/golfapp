/**
 * Server
 * - fire up server and handle /search route
 * - call search.getStats and return the results from route
 *
 * ffriel
 * 17/03/2020
 */
const express = require('express');
const app = express();
const cors = require('cors');

const Search = require('./search');

const port = 3001;

const search = new Search({
    projectId: 'api-project-268416',
    region: 'us-central1',
    clusterName: 'hive-cluster',
});

app.use(cors());

app.get('/search', (req, res) => {
    const player = req.query.player;
    const variable = req.query.variable;

    if (!player || !variable) {
        console.error('Both Player and Variable are expected.');
        res.sendStatus(400);
    } else {
        search.getStats(player, variable).then(function(results) {
            if (results.error) {
                res.sendStatus(500);
            } else {
                res.send(results);
            }
        });
    }
});

app.listen(port, () => console.log(`PGA Golf backend app listening on port ${port}!`));
