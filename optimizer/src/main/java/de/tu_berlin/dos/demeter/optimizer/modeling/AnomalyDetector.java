package de.tu_berlin.dos.demeter.optimizer.modeling;

import de.tu_berlin.dos.demeter.optimizer.structures.Observation;
import de.tu_berlin.dos.demeter.optimizer.structures.TimeSeries;
import iftm.anomalydetection.AutoMLAnomalyDetection;
import iftm.anomalydetection.DistancePredictionResult;
import iftm.automl.identitifunction.AbstractArimaBreeding;
import iftm.automl.identitifunction.ArimaMultiBreeding;
import iftm.automl.thresholdmodel.CompleteHistoryThresholdBreedingPart;
import iftm.automl.thresholdmodel.ThresholdBreedingPart;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AnomalyDetector {

    private static final Logger LOG = LogManager.getLogger(AnomalyDetector.class);
    private static final double TOLERANCE = 15.0;

    private final AutoMLAnomalyDetection detector;
    private boolean trained = false;

    public AnomalyDetector() {

        ThresholdBreedingPart breeding1 = new CompleteHistoryThresholdBreedingPart(5.0);
        AbstractArimaBreeding breeding2 = new ArimaMultiBreeding(50, 4, 200, breeding1);
        this.detector = new AutoMLAnomalyDetection(breeding2, 1000, false);
    }

    public AnomalyDetector train(List<TimeSeries> trainSet, long trainSize) {

        // extract values for each timestamp and train model iteratively until all data points are consumed
        for (int i = 0; i < trainSize; i++) {

            boolean proceed = true;
            double[] currDataPoints = new double[trainSet.size()];
            // retrieve data points for each timestamp based on index
            for (int j = 0; j < trainSet.size(); j++) {

                TimeSeries series = trainSet.get(j);
                Double value = series.getObservations().get(i).value;
                if (value == null || Double.isNaN(value)) {

                    proceed = false;
                    break;
                }
                else currDataPoints[j] = value;
            }
            // if no NaN values were detected in training points, then train the detector
            if (proceed) this.detector.train(currDataPoints);
        }
        this.trained = true;
        LOG.info("Training finished");
        return this;
    }

    public double measure(List<TimeSeries> dataset, long dataSize) {

        if (!this.trained) throw new IllegalStateException("Train the Anomaly Detector first");

        double duration = 0.0;
        double history = 0.0;

        // extract data points for current timestamp
        for (int i = 0; i < dataSize; i++) {

            boolean proceed = true;
            double[] currDataPoints = new double[dataset.size()];
            for (int j = 0; j < dataset.size(); j++) {

                TimeSeries series = dataset.get(j);
                Double value = series.getObservations().get(i).value;
                if (value == null || Double.isNaN(value)) {

                    proceed = false;
                    history = 0.0;
                    duration += 1.0;
                    break;
                }
                else currDataPoints[j] = value;
            }
            // proceed only if no NaN values were found
            if (proceed) {

                DistancePredictionResult result = detector.predict(currDataPoints);
                if (result.isAnomaly()) {

                    history = 0.0;
                    duration += 1.0;
                }
                else if (!result.isAnomaly()) {
                    // if tolerance is exceeded, then break (its very likely that the anomaly actually terminated)
                    if (TOLERANCE < history) return duration - history;
                    // some tolerance (numbers of datapoints), i.e. if its actually an anomaly but the detector failed
                    else {

                        history += 1.0;
                        duration += 1.0;
                    }
                }
            }
        }
        return -1.0;
    }

}
