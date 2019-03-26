import events from 'girder/events';
import router from 'girder/router';
import { restRequest } from 'girder/rest';
import { wrap } from 'girder/utilities/PluginUtils';
import FilesystemImportView from 'girder/views/body/FilesystemImportView';
import ItemView from 'girder/views/body/ItemView';
import importTemplate from './import.pug';
import './import.styl';
import 'girder/utilities/jquery/girderEnable';


wrap(FilesystemImportView, 'render', function (render) {
    render.call(this);
    this.$('.g-submit-assetstore-import').after(importTemplate());
    return this;
});

FilesystemImportView.prototype.events['click .g-hdf5-import'] = function (e) {
    e.preventDefault();

    $(e.target).girderEnable(false);
    restRequest({
        type: 'POST',
        url: `assetstore/${this.assetstore.id}/hdf5_import`,
        data: {
            folderId: this.$('#g-filesystem-import-dest-id').val().split(' ')[0],
            path: this.$('#g-filesystem-import-path').val(),
            progress: true
        },
        error: null,
    }).done(() => {
        events.trigger('g:alert', {
            icon: 'ok',
            type: 'success',
            text: 'Import complete.',
            timeout: 4000,
        });
    }).error((resp) => {
        events.trigger('g:alert', {
            icon: 'cancel',
            text: resp.responseJSON.message,
            type: 'danger',
            timeout: 4000
        });
    }).always(() => {
        $(e.target).girderEnable(true);
    })
};
