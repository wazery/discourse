/**
  Our data model for a color within a color scheme.
  (It's a funny name for a class, but Color seemed too generic for what this class is.)

  @class ColorSchemeColor
  @extends Discourse.Model
  @namespace Discourse
  @module Discourse
**/
Discourse.ColorSchemeColor = Discourse.Model.extend({
  init: function() {
    this._super();
    this.startTrackingChanges();
  },

  startTrackingChanges: function() {
    this.set('originals', {
      hex: this.get('hex') || 'FFFFFF',
      opacity: this.get('opacity') || '100'
    });
    this.notifyPropertyChange('hex'); // force changed property to be recalculated
  },

  changed: function() {
    if (!this.originals) return false;

    if (this.get('hex') !== this.originals['hex'] || this.get('opacity').toString() !== this.originals['opacity'].toString()) {
      return true;
    } else {
      return false;
    }
  }.property('hex', 'opacity'),

  undo: function() {
    if (this.originals) {
      this.set('hex',     this.originals['hex']);
      this.set('opacity', this.originals['opacity']);
    }
  }
});
