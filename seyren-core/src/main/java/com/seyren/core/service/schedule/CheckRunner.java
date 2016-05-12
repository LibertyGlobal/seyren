/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.seyren.core.service.schedule;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.seyren.core.domain.Alert;
import com.seyren.core.domain.AlertType;
import com.seyren.core.domain.Check;
import com.seyren.core.domain.Subscription;
import com.seyren.core.service.checker.TargetChecker;
import com.seyren.core.service.checker.ValueChecker;
import com.seyren.core.service.notification.NotificationService;
import com.seyren.core.service.notification.NotificationServiceSettings;
import com.seyren.core.store.AlertsStore;
import com.seyren.core.store.ChecksStore;
import com.seyren.core.util.config.SeyrenConfig;

public class CheckRunner implements Runnable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckRunner.class);
    
    private final Check check;
    private final AlertsStore alertsStore;
    private final ChecksStore checksStore;
    private final TargetChecker targetChecker;
    private final ValueChecker valueChecker;
    private final Iterable<NotificationService> notificationServices;
    private final SeyrenConfig seyrenConfig;
    private final NotificationServiceSettings notificationServiceSettings;
    private Boolean alarmNotificationIsSent = false;
    
    public CheckRunner(Check check, AlertsStore alertsStore, ChecksStore checksStore, TargetChecker targetChecker, ValueChecker valueChecker,
            Iterable<NotificationService> notificationServices, SeyrenConfig seyrenConfig, NotificationServiceSettings notificationServiceSettings) {
        this.check = check;
        this.alertsStore = alertsStore;
        this.checksStore = checksStore;
        this.targetChecker = targetChecker;
        this.valueChecker = valueChecker;
        this.notificationServices = notificationServices;
        this.seyrenConfig = seyrenConfig;
        this.notificationServiceSettings = notificationServiceSettings;
    }
    
    @Override
    public final void run() {
        if (!check.isEnabled()) {
            return;
        }
        
        try {
            System.out.println("Check Runner");
            Map<String, Optional<BigDecimal>> targetValues = targetChecker.check(check);
            DateTime now = new DateTime();
            BigDecimal warn = check.getWarn();
            BigDecimal error = check.getError();
            BigDecimal singleCheckNotificationDelayInSeconds = check.getNotificationDelay();
            Integer globalNofiticationDelayInSeconds = seyrenConfig.getAlertNotificationDelayInSeconds();
            
            AlertType worstState;
//            worstState = AlertType.OK;
            if (check.isAllowNoData()) {
                worstState = AlertType.OK;
            } else {
                worstState = AlertType.UNKNOWN;
            }
            
            //Remember this is CHECK state not ALERT state
            //System.out.println(check.getState());
//            System.out.println(worstState.toString());
            
            List<Alert> interestingAlerts = new ArrayList<Alert>();
            Integer alertsInErrorCounter = 0;
            Integer alertsNotInErrorCounter = 0;
            
            for (Entry<String, Optional<BigDecimal>> entry : targetValues.entrySet()) {
                String target = entry.getKey();
                Optional<BigDecimal> value = entry.getValue();

                if (!value.isPresent()) {
                    if (!check.isAllowNoData()) {
                        LOGGER.warn("No value present for {} and check must have data", target);
                    }
                    continue;
                }
                
                BigDecimal currentValue = value.get();
                
                Alert lastAlert = alertsStore.getLastAlertForTargetOfCheck(target, check.getId());
                
                AlertType lastState;
                
                if (lastAlert == null) {
                    lastState = AlertType.OK;
                } else {
                    lastState = lastAlert.getToType();
                }
                
                AlertType currentState = valueChecker.checkValue(currentValue, warn, error);
                          
                if (currentState == AlertType.ERROR || currentState == AlertType.WARN) {
                    alertsInErrorCounter = targetValues.size() - (targetValues.size() - (alertsInErrorCounter + 1));
                } else {
                    alertsNotInErrorCounter = targetValues.size() - alertsInErrorCounter;
                }
                
                if (currentState.isWorseThan(worstState)) {
                    worstState = currentState;
                }
                
                if (isStillOk(lastState, currentState)) {
                    continue;
                }
                
                Alert alert = createAlert(target, currentValue, warn, error, lastState, currentState, now);
                
                alertsStore.createAlert(check.getId(), alert);
                
                Boolean sendNotification = false;
                
                // OK So, the error is in your own code. For some reason this second target always gets a FALSE from the notification settings... You need to find out why..
                // ALSO - remember to make sure Notifications between ERROR and OK are NOT sent anymore.
                if (singleCheckNotificationDelayInSeconds != null) {
                    sendNotification = notificationServiceSettings.applyNotificationDelayAndIntervalProperties(check, lastState, currentState, now);
                } else if(globalNofiticationDelayInSeconds != 0) {
                    sendNotification = notificationServiceSettings.applyNotificationDelayAndIntervalProperties(check, lastState, currentState, now);
                } else if(!stateIsTheSame(lastState, currentState)) {
                    sendNotification = true;
                }
                
//                System.out.println(alertsInErrorCounter);
//                System.out.println(alertsNotInErrorCounter);
//                System.out.println(targetValues.size());
                
//                if ((alertsInErrorCounter + alertsNotInErrorCounter) == targetValues.size()) {
//                    check.setTimeLastNotificationSent(now);
//                }

                if (sendNotification) {
                    System.out.println("Notification will be sent");
                    interestingAlerts.add(alert);
                    //lastNotificationSent = true;
//                    if (check.getTimeLastNotificationSent() == null) {
//                        System.out.println("Last notification has NOT yet been sent");
//                        interestingAlerts.add(alert);
//                    } else if (currentState == AlertType.OK) {
//                        interestingAlerts.add(alert);
//                        check.setTimeLastNotificationSent(null);
//                    }             
                }
            }
            
            Check updatedCheck = checksStore.updateStateAndLastCheck(check.getId(), worstState, DateTime.now());
            
            System.out.println(check.getName());
            System.out.println(interestingAlerts.size());
            System.out.println(check.getState());
            System.out.println(check.getTimeLastNotificationSent() == null);
            System.out.println(updatedCheck.getState());
            
            if (interestingAlerts.isEmpty()) {
                return;
            }
            
            for (Subscription subscription : updatedCheck.getSubscriptions()) {
                if (!subscription.shouldNotify(now, worstState)) {
                    continue;
                }
                
                for (NotificationService notificationService : notificationServices) {
                    if (notificationService.canHandle(subscription.getType())) {
                        try {
                            System.out.println("hierin");
                            if (!check.errorNotificationIsSent()) {
                                System.out.println("Send ERROR Notification");
                                check.setTimeLastNotificationSent(now);
                                check.setErrorNotificationIsSent(true);
                                checksStore.updateTimeLastNotification(check.getId(), now, true);
                                notificationService.sendNotification(updatedCheck, subscription, interestingAlerts);
                            } else if (updatedCheck.getState() == AlertType.OK) {
                                System.out.println("Send OK Notification");
                                System.out.println(interestingAlerts.size());
                                notificationService.sendNotification(updatedCheck, subscription, interestingAlerts);
                                check.setTimeLastNotificationSent(null);
                                System.out.println("adjust object");
                                check.setErrorNotificationIsSent(false);
                                checksStore.updateTimeLastNotification(check.getId(), now, false);
                                System.out.println("database save");
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Notifying {} by {} failed.", subscription.getTarget(), subscription.getType(), e);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            LOGGER.warn("{} failed", check.getName(), e);
        }
    }
    
    private boolean isStillOk(AlertType last, AlertType current) {
        return last == AlertType.OK && current == AlertType.OK;
    }
    
    private boolean stateIsTheSame(AlertType last, AlertType current) {
        return last == current;
    }
    
    private Alert createAlert(String target, BigDecimal value, BigDecimal warn, BigDecimal error, AlertType from, AlertType to, DateTime now) {
        return new Alert()
                .withTarget(target)
                .withValue(value)
                .withWarn(warn)
                .withError(error)
                .withFromType(from)
                .withToType(to)
                .withTimestamp(now);
    }
}