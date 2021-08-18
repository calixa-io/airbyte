import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import NotificationsForm from "./components/NotificationsForm";
import useWorkspace from "components/hooks/services/useWorkspace";
import WebHookForm from "./components/WebHookForm";
import HeadTitle from "components/HeadTitle";
import useWorkspaceEditor from "pages/SettingsPage/components/useWorkspaceEditor";

import { Content, SettingsCard } from "../SettingsComponents";

const NotificationPage: React.FC = () => {
  const { workspace, updateWebhook, testWebhook } = useWorkspace();
  const {
    errorMessage,
    successMessage,
    loading,
    updateData,
  } = useWorkspaceEditor();
  const [
    errorWebhookMessage,
    setErrorWebhookMessage,
  ] = useState<React.ReactNode>(null);
  const [
    successWebhookMessage,
    setSuccessWebhookMessage,
  ] = useState<React.ReactNode>(null);

  const onChange = async (data: {
    news: boolean;
    securityUpdates: boolean;
  }) => {
    await updateData({ ...workspace, ...data });
  };

  const onSubmitWebhook = async (data: {
    webhook: string;
    sendOnSuccess: boolean;
    sendOnFailure: boolean;
  }) => {
    setSuccessWebhookMessage(null);
    setErrorWebhookMessage(null);
    try {
      await updateWebhook(data);
      setSuccessWebhookMessage(<FormattedMessage id="settings.changeSaved" />);

      setTimeout(() => {
        setSuccessWebhookMessage(null);
      }, 2000);
    } catch (e) {
      setErrorWebhookMessage(<FormattedMessage id="form.someError" />);

      setTimeout(() => {
        setErrorWebhookMessage(null);
      }, 2000);
    }
  };

  const onTestWebhook = async (data: {
    webhook: string;
    sendOnSuccess: boolean;
    sendOnFailure: boolean;
  }) => {
    await testWebhook(data.webhook, data.sendOnSuccess, data.sendOnFailure);
  };

  const initialWebhookUrl =
    workspace.notifications && workspace.notifications.length
      ? workspace.notifications[0].slackConfiguration.webhook
      : "";
  const initialSendOnSuccess =
    workspace.notifications && workspace.notifications.length
      ? workspace.notifications[0].slackConfiguration.sendOnSuccess
      : false;
  const initialSendOnFailure =
    workspace.notifications && workspace.notifications.length
      ? workspace.notifications[0].slackConfiguration.sendOnFailure
      : false;

  return (
    <>
      <HeadTitle
        titles={[{ id: "sidebar.settings" }, { id: "settings.notifications" }]}
      />
      <SettingsCard
        title={<FormattedMessage id="settings.notificationSettings" />}
      >
        <Content>
          <WebHookForm
            webhook={{
              notificationUrl: initialWebhookUrl,
              sendOnSuccess: initialSendOnSuccess,
              sendOnFailure: initialSendOnFailure,
            }}
            onSubmit={onSubmitWebhook}
            onTest={onTestWebhook}
            errorMessage={errorWebhookMessage}
            successMessage={successWebhookMessage}
          />

          <NotificationsForm
            isLoading={loading}
            errorMessage={errorMessage}
            successMessage={successMessage}
            onChange={onChange}
            preferencesValues={{
              news: workspace.news,
              securityUpdates: workspace.securityUpdates,
            }}
          />
        </Content>
      </SettingsCard>
    </>
  );
};

export default NotificationPage;
