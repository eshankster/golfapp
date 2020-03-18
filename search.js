/**
 * Search
 * - all the heavy work occurs here
 * - set up cluster client
 * - set up job controller client
 * - on each call
 *   - configure job options (incl. sql)
 *   - keep checking job status until DONE
 *   - once DONE pull the result from cloud storage
 *   - timeout after 600000 milliseconds if DONE never returns
 *
 * ffriel
 * 17/03/2020
 */
const dataproc = require("@google-cloud/dataproc");
const {
    Storage
} = require('@google-cloud/storage');
const sleep = require("sleep");

let clusterClient = {},
    jobClient = {},
    region = '',
    projectId = '',
    clusterName = '';

class Search {
    constructor(opts) {
        region = opts.region;
        projectId = opts.projectId;
        clusterName = opts.clusterName;

        // Create a cluster client with the endpoint set to the desired cluster region
        clusterClient = new dataproc.v1.ClusterControllerClient({
            apiEndpoint: region + `-dataproc.googleapis.com`,
            keyFilename: './account.json',
        });

        // Create a job client with the endpoint set to the desired cluster region
        jobClient = new dataproc.v1.JobControllerClient({
            apiEndpoint: region + `-dataproc.googleapis.com`,
            keyFilename: './account.json',
        });
    }

    async getStats(player, variable) {
        try {
            const job = {
                projectId: projectId,
                region: region,
                job: {
                    placement: {
                        clusterName: clusterName
                    },
                    hiveJob: {
                        "queryList": {
                            "queries": [
                                "select * from pga where playername = '" + player + "' and variable = '" + variable + "';",
                            ]
                        }
                    }
                }
            };

            let [jobResp] = await jobClient.submitJob(job);
            const jobId = jobResp.reference.jobId;

            console.log(`Submitted job "${jobId}".`);

            // Terminal states for a job
            const terminalStates = new Set(['DONE', 'ERROR', 'CANCELLED']);

            // Create a timeout such that the job gets cancelled if not
            // in a termimal state after a fixed period of time.
            const timeout = 600000;
            const start = new Date();

            // Wait for the job to finish.
            const jobReq = {
                projectId: projectId,
                region: region,
                jobId: jobId,
            };

            while (!terminalStates.has(jobResp.status.state)) {
                if (new Date() - timeout > start) {
                    await jobClient.cancelJob(jobReq);
                    console.log(
                        `Job ${jobId} timed out after threshold of ` +
                        `${timeout / 60000} minutes.`
                    );
                    break;
                }
                await sleep.sleep(1);
                [jobResp] = await jobClient.getJob(jobReq);
            }

            const clusterReq = {
                projectId: projectId,
                region: region,
                clusterName: clusterName,
            };

            const [clusterResp] = await clusterClient.getCluster(clusterReq);

            const storage = new Storage({
                keyFilename: './account.json',
            });

            const output = await storage
                .bucket(clusterResp.config.configBucket)
                .file(
                    `google-cloud-dataproc-metainfo/${clusterResp.clusterUuid}/` +
                    `jobs/${jobId}/driveroutput.000000000`
                )
                .download();

            // Output a success message.
            console.log(
                `Job ${jobId} finished with state ${jobResp.status.state}:\n${output}`
            );
            
            return output;
        } catch (e) {
            console.error('Error:', e);
            return ({
                error: 'Search failed.',
            });
        }
    }
}

module.exports = Search;